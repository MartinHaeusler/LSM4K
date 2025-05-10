package org.chronos.chronostore.lsm.compaction.tasks

import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.mainTask
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTaskWithMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.impl.Killswitch
import org.chronos.chronostore.lsm.FlushResult
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.util.logging.LogExtensions.ioDebug
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import org.chronos.chronostore.util.unit.BinarySize.Companion.Bytes
import java.util.concurrent.atomic.AtomicLong

class FlushInMemoryTreeToDiskTask(
    private val lsmTree: LSMTree,
    private val killswitch: Killswitch,
    private val scheduleMinorCompactionOnCompletion: Boolean,
) : AsyncTask {

    companion object {

        private val log = KotlinLogging.logger {}

        private val counter = AtomicLong(0)

    }

    private val index = counter.getAndIncrement()

    override val name: String
        get() = "Flushing LSM Tree to Disk [${this.index}]: ${lsmTree.path}"

    override fun run(monitor: TaskMonitor) = monitor.mainTask(this.name) {
        log.ioDebug { "FLUSH TASK [${this.index}] START on ${this.lsmTree}" }
        try {
            val flushResult = monitor.subTaskWithMonitor(1.0) { subMonitor ->
                lsmTree.flushInMemoryDataToDisk(
                    minFlushSize = 0.Bytes,
                    monitor = subMonitor,
                )
            }
            logTaskResult(flushResult)
            updateStatistics(flushResult)
            if (scheduleMinorCompactionOnCompletion) {
                // we've just flushed data to disk; schedule a minor
                // compaction that allows the tree to re-organize itself.
                // In the worst case, this will be a no-op and we wasted
                // a couple of CPU cycles, but in the best case, this
                // helps to reduce read amplification. Note that we SCHEDULE
                // the minor compaction, we do not execute it synchronously
                // here because we want the flush task to finish as quickly
                // as possible since we may be blocking writers waiting for
                // memtable space to become available.
                this.lsmTree.scheduleMinorCompaction()
            }
            Unit
        } catch (t: Throwable) {
            killswitch.panic("An unexpected error occurred during Flush of store '${lsmTree.storeId}': ${t}", t)
            throw t
        }
    }

    private fun logTaskResult(flushResult: FlushResult?) {
        if (flushResult == null || flushResult.bytesWritten <= 0) {
            log.ioDebug { "FLUSH TASK [${this.index}] DONE on ${this.lsmTree.storeId} - no data needed to be written." }
        } else {
            log.ioDebug {
                val writtenBytes = flushResult.bytesWritten
                val bytesPerSecond = flushResult.throughputPerSecond
                val entries = flushResult.entriesWritten
                "FLUSH TASK [${this.index}] DONE on ${this.lsmTree.storeId}." +
                    " Wrote ${entries} entries to disk (file size: ${writtenBytes.Bytes.toHumanReadableString()})" +
                    " with ${bytesPerSecond.Bytes.toHumanReadableString()}/s. Target file: ${flushResult.targetFile?.path}"
            }
        }
    }

    private fun updateStatistics(flushResult: FlushResult?) {
        if (flushResult == null) {
            return
        }
        ChronoStoreStatistics.FLUSH_TASK_EXECUTIONS.incrementAndGet()
        ChronoStoreStatistics.FLUSH_TASK_WRITTEN_BYTES.getAndAdd(flushResult.bytesWritten)
        ChronoStoreStatistics.FLUSH_TASK_WRITTEN_ENTRIES.getAndAdd(flushResult.entriesWritten.toLong())
        ChronoStoreStatistics.FLUSH_TASK_TOTAL_TIME.getAndAdd(flushResult.runtimeMillis)
    }

}