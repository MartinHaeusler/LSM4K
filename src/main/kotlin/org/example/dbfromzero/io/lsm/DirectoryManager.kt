package org.example.dbfromzero.io.lsm

import mu.KotlinLogging
import org.example.dbfromzero.io.vfs.VirtualDirectory
import org.example.dbfromzero.io.vfs.VirtualFile
import org.example.dbfromzero.io.vfs.VirtualReadWriteFile
import org.example.dbfromzero.util.Bytes
import java.io.BufferedInputStream
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Manages access to a [directory][VirtualDirectory] and ensures proper locking of concurrent operations.
 */
class DirectoryManager(
    /** The directory assigned to this manager.*/
    val directory: VirtualDirectory,
) {

    companion object {

        private val log = KotlinLogging.logger {}

        private const val BUFFER_SIZE = 0x4000

    }

    /**
     * The base SSTable file of the LSM tree.
     *
     * Eventually, all deltas will be merged into this file.
     */
    val baseFile: VirtualFile = this.directory.file("base")

    /**
     * The base index file for the LSM tree.
     */
    val baseIndex: VirtualReadWriteFile = this.directory.file("base-index")

    private val deltas: TreeMap<Int, LockProtectedReader> = TreeMap()

    private var baseReader: LockProtectedReader? = null

    private var nextDelta = 0


    fun baseFileExists(): Boolean {
        return this.baseFile.exists()
    }

    fun isBaseInUse(): Boolean {
        return this.baseReader != null
    }

    @Synchronized
    fun getOrderedDeltasInUse(): List<Int> {
        return this.deltas.keys.toList()
    }

    fun searchForKey(key: Bytes): Bytes? {
        check(isBaseInUse())
        // get the list of deltas. This list is sorted ASCENDING (the oldest delta first)
        val orderedDeltas = synchronized(this) {
            this.deltas.values.toList()
        }
        // search for the key in each delta (from newest to oldest) and finally in the base
        val deltaValue = searchForEntryInDeltas(orderedDeltas.asReversed().iterator(), key)
        if (deltaValue != null) {
            // we've found the key in a delta file
            return deltaValue
        }
        // we haven't found the key in any of the delta files, check the base file
        return baseReader!!.withReadLock { it!!.get(key) }
    }

    private fun searchForEntryInDeltas(iterator: Iterator<LockProtectedReader>, key: Bytes): Bytes? {
        if (!iterator.hasNext()) {
            // no more files to check and key was not found yet
            return null
        }
        val lockProtectedReader = iterator.next()
        // Note: it is possible the delta was deleted while we were searching other
        // deltas or while we were waiting for the read lock.
        // That is fine because now the delta has been merged with the base, and we always
        // search the deltas first. If the key was in the delta and the delta was removed
        // before we had a chance to read it, then we'll find the entry when we search the base.
        return lockProtectedReader.withReadLock { it?.get(key) }
    }

    // this methods should only be called by the delta writer coordinator
    fun setBase() {
        log.info { "Setting initial base for '${this.baseFile.path}'." }
        check(!isBaseInUse())
        val reader = createRandomAccessReader(this.baseFile, this.baseIndex)
        synchronized(this) {
            baseReader = LockProtectedReader(reader)
        }
    }

    // this methods should only be called by the delta writer coordinator
    @Synchronized
    fun allocateDelta(): Int {
        return nextDelta++
    }

    // this methods should only be called by the delta writer coordinator
    @Synchronized
    fun addDelta(delta: Int) {
        check(isBaseInUse())
        this.deltas[delta] = LockProtectedReader(createRandomAccessReader(getDeltaFile(delta), getDeltaIndexFile(delta)))
    }

    // this methods should only be called by the merger cron
    fun getBaseReaderForMerge(): RandomAccessKeyValueFileReader {
        check(isBaseInUse())
        return baseReader!!.reader!!
    }

    // this methods should only be called by the merger cron
    fun commitNewBase(
        baseDataOverWriter: VirtualReadWriteFile.OverWriter,
        baseIndexOverWriter: VirtualReadWriteFile.OverWriter
    ) {
        log.info { "Committing new base to '${this.baseFile.path}'" }
        check(isBaseInUse())
        // this operation can take a non-trivial amount of time since we have to read the index
        // ideally we could load the index from temp file or key it in memory while writing, but
        // I don't think that is a worthwhile optimization, although it will cause all other base
        // operations to block
        baseReader!!.withWriteLock {
            baseDataOverWriter.commit()
            baseIndexOverWriter.commit()
            baseReader!!.reader = createRandomAccessReader(baseFile, baseIndex)
        }
    }

    // this methods should only be called by the merger cron
    fun deleteDelta(delta: Int) {
        log.info { "Deleting delta #${delta} in '${this.baseFile.path}'" }
        val wrapper = synchronized(this) {
            deltas.remove(delta)
        }
        requireNotNull(wrapper) { "Delta #${delta} is not in use in '${this.baseFile.path}'!" }
        wrapper.withWriteLock {
            wrapper.reader = null // mark the reader as deleted just in case that another thread acquires a read lock after this
            getDeltaFile(delta).delete()
            getDeltaIndexFile(delta).delete()
        }
    }

    fun getDeltaFile(delta: Int): VirtualReadWriteFile {
        require(delta >= 0) { "Delta values must be positive, but got: $delta" }
        return this.directory.file("delta-$delta")
    }

    fun getDeltaIndexFile(delta: Int): VirtualReadWriteFile {
        require(delta >= 0) { "Delta values must be positive, but got: $delta" }
        return this.directory.file("delta-$delta-index")
    }

    private fun createBaseReader(): KeyValueFileReader {
        return createReader(this.baseFile)
    }

    private fun deltaReader(delta: Int): KeyValueFileReader {
        return createReader(getDeltaFile(delta))
    }

    private fun createReader(file: VirtualFile): KeyValueFileReader {
        return KeyValueFileReader(BufferedInputStream(file.createInputStream(), BUFFER_SIZE))
    }

    private fun createRandomAccessReader(dataFile: VirtualFile, indexFile: VirtualFile): RandomAccessKeyValueFileReader {
        return RandomAccessKeyValueFileReader(dataFile, RandomAccessKeyValueFileReader.readIndexAndClose(indexFile))
    }


    private class LockProtectedReader(
        var reader: RandomAccessKeyValueFileReader?,
    ) {

        private val lock = ReentrantReadWriteLock(true)

        inline fun <T> withReadLock(action: (RandomAccessKeyValueFileReader?) -> T): T {
            this.lock.read {
                return action(reader)
            }
        }

        inline fun <T> withWriteLock(action: (RandomAccessKeyValueFileReader?) -> T): T {
            this.lock.write {
                return action(reader)
            }
        }

    }

}