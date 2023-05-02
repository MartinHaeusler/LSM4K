package org.chronos.chronostore.lsm.merge.strategy

import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.async.executor.AsyncTaskManager
import org.chronos.chronostore.lsm.event.InMemoryLsmInsertEvent
import org.chronos.chronostore.lsm.event.LsmCursorClosedEvent
import org.chronos.chronostore.lsm.merge.tasks.FlushInMemoryTreeToDiskTask

class DefaultMergeService(
    private val taskManager: AsyncTaskManager,
    private val storeConfig: ChronoStoreConfiguration,
): MergeService {


    override fun initialize() {
        // this.taskManager.scheduleRecurringWithTimeBetweenExecutions()
    }

    override fun handleInMemoryInsertEvent(event: InMemoryLsmInsertEvent) {
        if(event.lsmTree.inMemorySizeBytes < this.storeConfig.maxInMemoryTreeSizeInBytes){
            return
        }
        // schedule a flush
        this.taskManager.executeAsync(FlushInMemoryTreeToDiskTask(event.lsmTree, this.storeConfig.maxInMemoryTreeSizeInBytes))
    }

    override fun handleCursorClosedEvent(lsmCursorClosedEvent: LsmCursorClosedEvent) {
    }


}