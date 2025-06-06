package org.lsm4k.lsm.garbagecollector.tasks

import org.lsm4k.api.StoreManager
import org.lsm4k.async.taskmonitor.TaskMonitor
import org.lsm4k.async.tasks.AsyncTask

class GarbageCollectorTask(
    private val storeManager: StoreManager
) : AsyncTask {

    override val name: String
        get() = "LSM Garbage Collector"

    override fun run(monitor: TaskMonitor) {
        this.storeManager.performGarbageCollection(monitor)
    }

}