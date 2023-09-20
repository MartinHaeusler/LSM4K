package org.chronos.chronostore.lsm.merge.tasks

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTaskWithMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.util.unit.BinarySize
import org.chronos.chronostore.util.unit.Bytes

class FlushInMemoryTreeToDiskTask(
    private val lsmTree: LSMTree,
) : AsyncTask {

    override val name: String
        get() = "Flushing LSM Tree to Disk: ${lsmTree.path}"

    override fun run(monitor: TaskMonitor) {
        monitor.reportStarted(this.name)
        println("FLUSH TASK START on tree '${this.lsmTree.path}'")
        monitor.subTaskWithMonitor(1.0) { subMonitor ->
            lsmTree.flushInMemoryDataToDisk(
                minFlushSize = 0.Bytes,
                monitor = subMonitor,
            )
        }
        println("FLUSH TASK DONE on tree '${this.lsmTree.path}'")
        monitor.reportDone()
    }

}