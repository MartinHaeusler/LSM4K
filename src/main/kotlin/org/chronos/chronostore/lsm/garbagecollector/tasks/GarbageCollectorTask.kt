package org.chronos.chronostore.lsm.garbagecollector.tasks

import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.tasks.AsyncTask

class GarbageCollectorTask(
    private val storeManager: StoreManager
) : AsyncTask {

    override val name: String
        get() = "LSM Garbage Collector"

    override fun run(monitor: TaskMonitor) {
        this.storeManager.performGarbageCollection(monitor)
    }

}