package org.chronos.chronostore.lsm.merge.strategy

import mu.KotlinLogging
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.async.executor.AsyncTaskManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.event.InMemoryLsmInsertEvent
import org.chronos.chronostore.lsm.event.LsmCursorClosedEvent
import org.chronos.chronostore.lsm.merge.tasks.CompactionTask
import org.chronos.chronostore.lsm.merge.tasks.FlushInMemoryTreeToDiskTask
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes

class MergeServiceImpl(
    private val taskManager: AsyncTaskManager,
    private val storeConfig: ChronoStoreConfiguration,
    private val storeManager: StoreManager,
) : MergeService {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    private val compactionTask = CompactionTask(this.storeManager, this.storeConfig.mergeStrategy)

    override fun initialize() {
        val timeBetweenExecutionsInMillis = this.storeConfig.mergeIntervalMillis
        if (timeBetweenExecutionsInMillis > 0) {
            this.taskManager.scheduleRecurringWithTimeBetweenExecutions(compactionTask, timeBetweenExecutionsInMillis.milliseconds)
        } else {
            log.warn { "Compaction is disabled, because the merge interval is <= 0!" }
        }
    }

    override fun mergeNow() {
        this.compactionTask.run(TaskMonitor.create())
    }

    override fun handleInMemoryInsertEvent(event: InMemoryLsmInsertEvent) {
        if (event.lsmTree.inMemorySizeBytes < this.storeConfig.maxInMemoryTreeSizeInBytes) {
            return
        }
        // schedule a flush
        this.taskManager.executeAsync(FlushInMemoryTreeToDiskTask(event.lsmTree, this.storeConfig.maxInMemoryTreeSizeInBytes))
    }

    override fun handleCursorClosedEvent(lsmCursorClosedEvent: LsmCursorClosedEvent) {
    }


}