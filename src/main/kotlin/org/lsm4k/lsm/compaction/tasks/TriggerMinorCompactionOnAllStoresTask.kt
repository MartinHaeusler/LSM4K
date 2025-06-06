package org.lsm4k.lsm.compaction.tasks

import org.lsm4k.api.Store
import org.lsm4k.async.taskmonitor.TaskMonitor
import org.lsm4k.async.taskmonitor.TaskMonitor.Companion.forEach
import org.lsm4k.async.tasks.AsyncTask
import org.lsm4k.impl.Killswitch
import java.util.concurrent.CompletableFuture

class TriggerMinorCompactionOnAllStoresTask(
    private val getAllStores: () -> List<Store>,
    private val killswitch: Killswitch,
) : AsyncTask {

    override val name: String
        get() = "Trigger Minor Compaction on all Stores"

    override fun run(monitor: TaskMonitor) {
        try {
            val allStores = this.getAllStores()

            val tasks = mutableListOf<CompletableFuture<*>>()
            monitor.forEach(1.0, "Triggering Minor Compaction on all Stores", allStores) { store ->
                tasks += store.scheduleMinorCompaction()
            }

            CompletableFuture.allOf(*tasks.toTypedArray()).exceptionally { t ->
                killswitch.panic("An unexpected error occurred when triggering Minor Compactions on all stores: ${t}", t)
                null
            }
        } catch (t: Throwable) {
            killswitch.panic("An unexpected error occurred when triggering Minor Compactions on all stores: ${t}", t)
        }
    }

}