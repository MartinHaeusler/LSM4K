package org.chronos.chronostore.lsm.merge.tasks

import mu.KotlinLogging
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTaskWithMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.util.log.LogMarkers
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import org.chronos.chronostore.util.unit.Bytes
import java.util.concurrent.atomic.AtomicLong

class FlushInMemoryTreeToDiskTask(
    private val lsmTree: LSMTree,
) : AsyncTask {

    companion object {

        private val log = KotlinLogging.logger {}

        private val counter = AtomicLong(0)

    }

    private val index = counter.getAndIncrement()

    override val name: String
        get() = "Flushing LSM Tree to Disk [${this.index}]: ${lsmTree.path}"

    override fun run(monitor: TaskMonitor) {
        monitor.reportStarted(this.name)
        log.debug(LogMarkers.IO) { "FLUSH TASK [${this.index}] START on ${this.lsmTree}" }
        val flushResult = monitor.subTaskWithMonitor(1.0) { subMonitor ->
            lsmTree.flushInMemoryDataToDisk(
                minFlushSize = 0.Bytes,
                monitor = subMonitor,
            )
        }
        if (flushResult.bytesWritten <= 0) {
            log.debug(LogMarkers.IO) { "FLUSH TASK [${this.index}] DONE on ${this.lsmTree.storeId} - no data needed to be written." }
        } else {
            log.debug(LogMarkers.IO) {
                val writtenBytes = flushResult.bytesWritten
                val bytesPerSecond = flushResult.throughputPerSecond
                val entries = flushResult.entriesWritten
                "FLUSH TASK [${this.index}] DONE on ${this.lsmTree.storeId}." +
                    " Wrote ${entries} entries to disk (file size: ${writtenBytes.Bytes.toHumanReadableString()})" +
                    " with ${bytesPerSecond.Bytes.toHumanReadableString()}/s. Target file: ${flushResult.targetFile?.path}"
            }
        }
        ChronoStoreStatistics.FLUSH_TASK_EXECUTIONS.incrementAndGet()
        ChronoStoreStatistics.FLUSH_TASK_WRITTEN_BYTES.getAndAdd(flushResult.bytesWritten)
        ChronoStoreStatistics.FLUSH_TASK_WRITTEN_ENTRIES.getAndAdd(flushResult.entriesWritten.toLong())
        ChronoStoreStatistics.FLUSH_TASK_TOTAL_TIME.getAndAdd(flushResult.runtimeMillis)
        monitor.reportDone()
    }

}