package org.chronos.chronostore.lsm.merge.tasks

import mu.KotlinLogging
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTaskWithMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.lsm.LSMTree
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

    init {
        log.trace { "Created flush task ${index}" }
    }

    override val name: String
        get() = "Flushing LSM Tree to Disk [${this.index}]: ${lsmTree.path}"

    override fun run(monitor: TaskMonitor) {
        monitor.reportStarted(this.name)
        log.info { "FLUSH TASK [${this.index}] START on ${this.lsmTree}" }
        val startTime = System.currentTimeMillis()
        val writtenBytes = monitor.subTaskWithMonitor(1.0) { subMonitor ->
            lsmTree.flushInMemoryDataToDisk(
                minFlushSize = 0.Bytes,
                monitor = subMonitor,
            )
        }
        if (writtenBytes <= 0) {
            log.info { "FLUSH TASK [${this.index}] DONE on ${this.lsmTree.storeId} - no data needed to be written." }
        } else {
            log.info {
                val endTime = System.currentTimeMillis()
                val totalTime = endTime - startTime
                val bytesPerSecond = (writtenBytes / (totalTime.toDouble() / 1000)).toInt()

                "FLUSH TASK [${this.index}] DONE on ${this.lsmTree.storeId}. Wrote ${writtenBytes.Bytes.toHumanReadableString()} to disk with ${bytesPerSecond.Bytes.toHumanReadableString()}/s."
            }
        }
        monitor.reportDone()
    }

}