package org.chronos.chronostore.lsm.merge.strategy

import org.chronos.chronostore.async.executor.AsyncTaskManager
import org.chronos.chronostore.lsm.event.InMemoryLsmInsertEvent
import org.chronos.chronostore.lsm.event.LsmCursorClosedEvent
import org.chronos.chronostore.lsm.merge.tasks.FlushInMemoryTreeToDiskTask

class DefaultMergeService(
    private val taskManager: AsyncTaskManager,
    private val maxInMemoryTreeSizeInBytes: Long,
): MergeService {


    override fun initialize() {
        this.taskManager.scheduleRecurringWithTimeBetweenExecutions()
    }

    override fun handleInMemoryInsertEvent(event: InMemoryLsmInsertEvent) {
        if(event.lsmTree.inMemorySizeBytes < this.maxInMemoryTreeSizeInBytes){
            return
        }
        // schedule a flush
        this.taskManager.executeAsync(FlushInMemoryTreeToDiskTask(event.lsmTree, this.maxInMemoryTreeSizeInBytes))
    }

    override fun handleCursorClosedEvent(lsmCursorClosedEvent: LsmCursorClosedEvent) {
    }


}