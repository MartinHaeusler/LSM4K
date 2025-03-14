package org.chronos.chronostore.lsm.compaction.tasks

import org.chronos.chronostore.api.Store
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.tasks.AsyncTask

class TriggerMajorCompactionOnAllStoresTask(
    val getAllStores: () -> List<Store>
): AsyncTask {

    override val name: String
        get() = "Trigger Minor Compaction on all Stores"

    override fun run(monitor: TaskMonitor) {
        val allStores = this.getAllStores()
        for(store in allStores){
            store.scheduleMajorCompaction()
        }
    }

}