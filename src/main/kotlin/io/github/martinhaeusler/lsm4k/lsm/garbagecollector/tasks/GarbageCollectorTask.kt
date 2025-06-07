package io.github.martinhaeusler.lsm4k.lsm.garbagecollector.tasks

import io.github.martinhaeusler.lsm4k.api.StoreManager
import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor
import io.github.martinhaeusler.lsm4k.async.tasks.AsyncTask

class GarbageCollectorTask(
    private val storeManager: StoreManager,
) : AsyncTask {

    override val name: String
        get() = "LSM Garbage Collector"

    override fun run(monitor: TaskMonitor) {
        this.storeManager.performGarbageCollection(monitor)
    }

}