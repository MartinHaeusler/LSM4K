package org.chronos.chronostore.lsm.merge.tasks

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.lsm.LSMTree

class FlushInMemoryTreeToDiskTask(
    private val lsmTree: LSMTree,
    private val maxInMemoryTreeSizeInBytes: Long,
): AsyncTask {

    override val name: String
        get() = "Flushing LSM Tree to Disk: ${lsmTree.path}"

    override fun run(monitor: TaskMonitor) {
        monitor.reportStarted(this.name)
        monitor.subMonitor(1.0)
        try{
            lsmTree.flushInMemoryDataToDisk(this.maxInMemoryTreeSizeInBytes, monitor)
        }finally{
            if(!monitor.status.state.isTerminal){
                monitor.reportDone()
            }
        }
    }

}