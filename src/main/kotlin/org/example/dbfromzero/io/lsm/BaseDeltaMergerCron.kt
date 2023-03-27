package org.example.dbfromzero.io.lsm

import mu.KotlinLogging
import org.example.dbfromzero.io.stream.PositionTrackingStream
import org.example.dbfromzero.io.vfs.VirtualFile
import org.example.dbfromzero.io.vfs.VirtualReadWriteFile
import org.example.dbfromzero.util.Bytes
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.time.Duration
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class BaseDeltaMergerCron(
    private val directoryManager: DirectoryManager,
    private val maxDeltaReadPercentage: Double,
    private val checkForDeltaRate: Duration,
    private val baseIndexRate: Int,
    private val executor: ScheduledExecutorService,
) {

    companion object {

        private val log = KotlinLogging.logger {}
        private const val BUFFER_SIZE = 0x4000

    }

    var hasErrors = false
    private var started = false
    private var shutdown = false
    private var checkFuture: ScheduledFuture<*>? = null

    init {
        require(maxDeltaReadPercentage > 0) { "Argument 'maxDeltaReadPercentage' must be greater than 0, but got: $maxDeltaReadPercentage!" }
        require(maxDeltaReadPercentage < 1) { "Argument 'maxDeltaReadPercentage' must be less than 1, but got: $maxDeltaReadPercentage!" }
    }

    @Synchronized
    fun start() {
        check(!this.started) { "The BaseDeltaMergerCron has already been started!" }
        this.checkFuture = executor.schedule(this::checkForDeltas, 0, TimeUnit.SECONDS)
        this.started = true
    }

    @Synchronized
    fun shutdown() {
        if (!started || shutdown) {
            return
        }
        checkFuture?.cancel(true)
        shutdown = true
    }

    private fun checkForDeltas() {
        if (shutdown || hasErrors) {
            return
        }
        try {
            checkForDeltasInternal()
            synchronized(this) {
                this.checkFuture = executor.schedule(this::checkForDeltas, checkForDeltaRate.toMillis(), TimeUnit.MILLISECONDS)
            }
        } catch (e: Exception) {
            log.error(e) { "Error in BaseDeltaMergerCron. Shutting down." }
            hasErrors = true
            checkFuture = null
            shutdown()
        }
    }

    private fun checkForDeltasInternal() {
        val orderedDeltasInUse = this.directoryManager.getOrderedDeltasInUse()
        if (orderedDeltasInUse.isEmpty()) {
            log.trace { "No deltas in use." }
            return
        }
        log.trace { "Found the following deltas in use: ${orderedDeltasInUse.joinToString()}" }
        check(this.directoryManager.isBaseInUse()) { "Base file is not in use in '${this.directoryManager.directory.path}'!" }

        val baseFile = this.directoryManager.baseFile
        val baseFileSize = baseFile.length
        val maxDeltaSize = (baseFileSize * maxDeltaReadPercentage / (1 - maxDeltaReadPercentage)).toLong()
        log.trace { "Base File of '${this.directoryManager.directory.path}' has size $baseFileSize bytes, max. delta size is $maxDeltaSize." }
        val orderedDeltaFilesForMerge = collectDeltaFilesForMerge(orderedDeltasInUse, maxDeltaSize)
        mergeDeltasAndCommit(orderedDeltaFilesForMerge)
        for (deltaPair in orderedDeltaFilesForMerge) {
            directoryManager.deleteDelta(deltaPair.key)
        }
    }

    private fun collectDeltaFilesForMerge(orderedDeltasInUse: List<Int>, maxDeltaSize: Long): Map<Int, VirtualReadWriteFile> {
        val deltaFilesForMerge = mutableMapOf<Int, VirtualReadWriteFile>()
        var sumDeltaSize = 0L
        // start with the oldest delta first (with the lowest index) and proceed in ascending index order.
        for (deltaIndex in orderedDeltasInUse) {
            val deltaFile = this.directoryManager.getDeltaFile(deltaIndex)
            val deltaSize = deltaFile.length
            log.trace { "Considering delta '${deltaFile.path}' of size $deltaSize bytes." }
            val sumIncludingDelta = sumDeltaSize + deltaSize
            if (sumIncludingDelta > maxDeltaSize) {
                if (deltaFilesForMerge.isNotEmpty()) {
                    // we've found at least some deltas to merge. Go ahead and do that.
                    break
                }
                // the oldest delta alone is enough to exceed our size limit. We HAVE to merge it anyway.
                log.warn { "Merging base '${this.directoryManager.baseFile.path}' with only the oldest delta in '${deltaFile.path}' (size: $deltaSize bytes) would exceed the configured delta size threshold of $maxDeltaSize bytes. Merging anyways." }
            }
            deltaFilesForMerge[deltaIndex] = deltaFile
            sumDeltaSize = sumIncludingDelta
        }
        return deltaFilesForMerge
    }

    private fun mergeDeltasAndCommit(orderedDeltaFilesForMerge: Map<Int, VirtualReadWriteFile>) {
        if (orderedDeltaFilesForMerge.isEmpty()) {
            return
        }
        log.info { "Merging base '${this.directoryManager.baseFile.path}' with the following deltas: ${orderedDeltaFilesForMerge.values.joinToString { it.path }}" }
        val baseFile = this.directoryManager.baseFile
        val baseIndex = this.directoryManager.baseIndex
        log.trace { "Total input size: ${Bytes.format(orderedDeltaFilesForMerge.values.sumOf { it.length })}" }
        // order readers as base and the deltas in descending age such that we prefer that last entry for a single key
        val orderedReaders = mutableListOf(batchReader(baseFile))
        for ((_, deltaFile) in orderedDeltaFilesForMerge) {
            orderedReaders += batchReader(deltaFile)
        }
        val selectedIterator = ValueSelectorIterator.createSortedAndSelectedIterator(orderedReaders)

        var baseOverWriter: VirtualReadWriteFile.OverWriter? = null
        var indexOverWriter: VirtualReadWriteFile.OverWriter? = null
        try {
            baseOverWriter = baseFile.createOverWriter()
            indexOverWriter = baseIndex.createOverWriter()
            PositionTrackingStream(baseOverWriter.outputStream, BUFFER_SIZE).use { outputStream ->
                KeyValueFileWriter(BufferedOutputStream(indexOverWriter.outputStream, BUFFER_SIZE)).use { indexWriter ->
                    writeMerged(selectedIterator, outputStream, indexWriter)
                }
            }
            this.directoryManager.commitNewBase(baseOverWriter, indexOverWriter)
        } catch (e: Exception) {
            baseOverWriter?.abort()
            indexOverWriter?.abort()
            throw RuntimeException("Error in merging deltas. Aborting.", e)
        } finally {
            for (reader in orderedReaders) {
                reader.close()
            }
        }
    }

    private fun writeMerged(
        selectedIterator: ValueSelectorIterator,
        outputStream: PositionTrackingStream,
        indexWriter: KeyValueFileWriter
    ) {
        val indexBuilder = IndexBuilder.create(indexWriter, this.baseIndexRate)
        var i = 0
        var count = 0
        KeyValueFileWriter(outputStream).use { writer ->
            while (selectedIterator.hasNext()) {
                if (this.shutdown) {
                    throw RuntimeException("Shutdown while merging. Aborting for fast exit.")
                }
                if (i.mod(10_000) == 0) {
                    if (Thread.interrupted()) {
                        Thread.currentThread().interrupt()
                        throw RuntimeException("Interrupted while merging. Aborting for fast exit.")
                    }
                    log.trace { "Writing merged entry #$i" }
                }
                i++
                val (key, value) = selectedIterator.next()
                if (value != DELETE_VALUE) {
                    indexBuilder.accept(outputStream.position, key)
                    writer.append(key, value)
                    count++
                }
            }
            log.trace { "Wrote $count key/value pairs into new base. ${i - count} entries were dropped due to deletions." }
        }
    }

    private fun batchReader(indexFile: VirtualFile): KeyValueFileReader {
        return KeyValueFileReader(BufferedInputStream(indexFile.createInputStream(), BUFFER_SIZE))
    }

}