package org.chronos.chronostore.lsm.garbagecollector.tasks

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.impl.ChronoStoreImpl

class GarbageCollectorTask(
    private val chronoStore: ChronoStoreImpl
) : AsyncTask {

    override val name: String
        get() = "LSM Garbage Collector"

    override fun run(monitor: TaskMonitor) {
        this.chronoStore.performGarbageCollection(monitor)
    }

}