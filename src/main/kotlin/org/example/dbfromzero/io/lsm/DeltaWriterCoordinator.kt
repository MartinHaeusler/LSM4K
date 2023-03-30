package org.example.dbfromzero.io.lsm

import mu.KotlinLogging
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.util.Bytes
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class DeltaWriterCoordinator(
    private val directoryManager: DirectoryManager,
    private val indexRate: Int,
    private val maxInFlightWriters: Int,
    private val executor: ScheduledExecutorService,
) {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    private val inFlightWriters = mutableListOf<WriteSortedEntriesJob>()
    private val lock = ReentrantReadWriteLock(true)
    private var writersCreated: Int = 0
    var anyWritesAborted: Boolean = false
    private var state: State = State.UNINITIALIZED

    init {
        require(indexRate > 0) { "Argument 'indexRate' must be positive, but got: $indexRate!" }
        require(maxInFlightWriters > 1) { "Argument 'maxInFlightWriters' must be greater than 1, but got: $maxInFlightWriters!" }
        require(maxInFlightWriters < 100) { "Argument 'maxInFlightWriters' must be less than 100, but got: $maxInFlightWriters!" }
    }

    val hasMaxInFlightWriters: Boolean
        get() = this.lock.read { this.inFlightWriters.size >= this.maxInFlightWriters }

    fun addWrites(originalWrites: Map<Bytes, Bytes>) {
        check(!hasMaxInFlightWriters)
        lock.write {
            if (this.state == State.UNINITIALIZED) {
                if (directoryManager.isBaseInUse()) {
                    state = State.WRITING_DELTAS
                } else {
                    log.info { "Creating new base for writes in '${this.directoryManager.directory.path}'." }
                    check(!directoryManager.baseFileExists()) {
                        "Could not create new base for writes: Base file already exists in '${this.directoryManager.directory.path}'!"
                    }
                    this.state = State.WRITING_BASE
                    createWriteJob(true, -1, originalWrites, directoryManager.baseFile, directoryManager.baseIndex)
                    return
                }
            }
            log.info { "Creating new delta for writes in '${this.directoryManager.directory.path}'." }
            val deltaIndex = this.directoryManager.allocateDelta()
            createWriteJob(false, deltaIndex, originalWrites, directoryManager.getDeltaFile(deltaIndex), directoryManager.getDeltaIndexFile(deltaIndex))
        }
    }

    fun searchForKeyInWritesInProgress(key: Bytes): Bytes? {
        this.lock.read {
            // search the newest writes first
            val iterator = inFlightWriters.asReversed().iterator()
            for (writer in iterator) {
                val value = writer.writes[key]
                if (value != null) {
                    return value
                }
            }
            return null
        }
    }

    private fun createWriteJob(
        isBase: Boolean,
        deltaIndex: Int,
        writes: Map<Bytes, Bytes>,
        dataFile: VirtualReadWriteFile,
        indexFile: VirtualReadWriteFile,
    ) {
        val job = WriteSortedEntriesJob(
            name = "write${this.writersCreated++}",
            isBase = isBase,
            deltaIndex = deltaIndex,
            indexRate = this.indexRate,
            writes = writes,
            dataFile = dataFile,
            indexFile = indexFile,
            coordinator = this
        )
        this.inFlightWriters += job
        this.executor.execute(job)
    }

    fun commitWrites(writer: WriteSortedEntriesJob) {
        if (this.anyWritesAborted) {
            log.warn { "Not committing writer '${writer.name}' since an earlier writer aborted!" }
            abortWrites(writer)
            return
        }

        lock.write {
            if (this.state == State.WRITING_BASE && !writer.isBase) {
                // we cannot write a delta before writing the base, so we wait
                log.info { "Writer '${writer.name}' finished before the initial base was finished. Will re-attempt to commit this later." }
                this.executor.schedule({ reattemptCommitWrites(writer, 1) }, 1, TimeUnit.SECONDS)
                return
            }
            try {
                if (this.state == State.WRITING_BASE) {
                    check(writer.isBase) { "Writer is attempting to write a delta but we're expecting it to write the base file!" }
                    this.directoryManager.setBase()
                    this.state = State.WRITING_DELTAS
                } else {
                    check(state == State.WRITING_DELTAS) { "DeltaWriterCoordinator for '${this.directoryManager.directory.path}' is uninitialized!" }
                    this.directoryManager.addDelta(writer.deltaIndex)
                }

                removeWriter(writer)
            } catch (e: Exception) {
                log.error(e) { "Error during the commit of writes of '${writer.name}' in DeltaWriterCoordinator for '${this.directoryManager.directory.path}'!" }
                abortWrites(writer)
            }
        }
    }

    private fun reattemptCommitWrites(writer: WriteSortedEntriesJob, count: Int) {
        log.info { "Reattempting write job '${writer.name}' (attempt #${count}) in ${this.directoryManager.directory.path}." }
        check(!writer.isBase) { "Cannot reattempt to write the base file. Writer: '${writer.name}', location: '${this.directoryManager.directory.path}'." }
        if (this.state == State.WRITING_BASE) {
            // TODO: having an upper limit on the number of reattempts is not ideal. What if the initial commit was huge?
            // Better idea would be to wait for the base writer to finish using a java.util.concurrent.Condition.
            if (count > 5) {
                log.warn { "Failed to commit '${writer.name}' after $count attempts. Aborting" }
                abortWrites(writer)
            } else {
                log.info { "Still writing base after $count attempts. Will reattempt commit of '${writer.name}'." }
                executor.schedule({ reattemptCommitWrites(writer, count + 1) }, (2 shl count).toLong(), TimeUnit.SECONDS)
            }
            return
        }
        // we're done writing the base directory, let's add our delta
        try {
            check(this.state == State.WRITING_DELTAS) { "DeltaWriterCoordinator for '${this.directoryManager.directory.path}' is uninitialized!" }
            commitWrites(writer)
        } catch (e: Exception) {
            log.error(e) { "Error in committing writes of job '${writer.name}' in ${this.directoryManager.directory.path}. Aborting." }
            abortWrites(writer)
        }
    }

    fun abortWrites(writer: WriteSortedEntriesJob) {
        this.anyWritesAborted = true
        log.warn { "Aborting writer '${writer.name}' in ${this.directoryManager.directory.path}." }
        removeWriter(writer)
    }

    private fun removeWriter(writer: WriteSortedEntriesJob) {
        this.lock.write {
            val removed = this.inFlightWriters.remove(writer)
            check(removed) { "Failed to remove in-flight writer '${writer.name}' in DeltaWriterCoordinator for '${this.directoryManager.directory.path}'!" }
        }
    }

    private enum class State {

        UNINITIALIZED,
        WRITING_BASE,
        WRITING_DELTAS,

    }
}