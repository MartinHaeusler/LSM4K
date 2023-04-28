package org.chronos.chronostore.lsm.merge.tasks

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.lsm.LSMTree

class CompactionTask(
    val lsmTree: LSMTree,
    val fileIndices: Set<Int>,
): AsyncTask {

    override val name: String
        get() = "Compact ${this.fileIndices.size} files in ${lsmTree.path}"

    override fun run(monitor: TaskMonitor) {
        monitor.reportStarted(this.name)
        monitor.subMonitor(1.0)
        try{
            this.lsmTree.mergeFiles(fileIndices, monitor)
        }finally{
            if(!monitor.status.state.isTerminal){
                monitor.reportDone()
            }
        }
    }
}