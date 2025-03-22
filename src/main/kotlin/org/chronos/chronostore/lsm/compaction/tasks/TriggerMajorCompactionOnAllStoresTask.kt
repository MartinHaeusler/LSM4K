package org.chronos.chronostore.lsm.compaction.tasks

import org.chronos.chronostore.api.Store
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.forEach
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.impl.Killswitch
import java.util.concurrent.CompletableFuture

class TriggerMajorCompactionOnAllStoresTask(
    private val getAllStores: () -> List<Store>,
    private val killswitch: Killswitch,
) : AsyncTask {

    override val name: String
        get() = "Trigger Major Compaction on all Stores"

    override fun run(monitor: TaskMonitor) {
        try {
            val allStores = this.getAllStores()
            val tasks = mutableListOf<CompletableFuture<*>>()
            monitor.forEach(1.0, "Triggering Major Compaction on all Stores", allStores) {
                tasks += it.scheduleMajorCompaction()
            }
            CompletableFuture.allOf(*tasks.toTypedArray()).exceptionally { t ->
                killswitch.panic("An unexpected error occurred when triggering Major Compactions on all stores: ${t}", t)
                null
            }
        } catch (t: Throwable) {
            killswitch.panic("An unexpected error occurred when triggering Major Compactions on all stores: ${t}", t)
            throw t
        }
    }

}