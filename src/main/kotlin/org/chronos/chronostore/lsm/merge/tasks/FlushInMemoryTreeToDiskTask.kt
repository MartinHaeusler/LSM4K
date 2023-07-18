package org.chronos.chronostore.lsm.merge.tasks

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTaskWithMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.util.unit.BinarySize

class FlushInMemoryTreeToDiskTask(
    private val lsmTree: LSMTree,
    private val maxInMemoryTreeSize: BinarySize,
) : AsyncTask {

    override val name: String
        get() = "Flushing LSM Tree to Disk: ${lsmTree.path}"

    override fun run(monitor: TaskMonitor) {
        monitor.reportStarted(this.name)
        println("FLUSH TASK START")
        monitor.subTaskWithMonitor(1.0) { subMonitor ->
            // the tree must be at least 33% full, otherwise we won't flush.
            lsmTree.flushInMemoryDataToDisk(
                minFlushSize = this.maxInMemoryTreeSize / 10,
                monitor = subMonitor
            )
        }
        println("FLUSH TASK DONE")
        monitor.reportDone()
    }

}