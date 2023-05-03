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

class MergeServiceImpl(
    private val taskManager: AsyncTaskManager,
    private val storeConfig: ChronoStoreConfiguration,
) : MergeService {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    private var initialized: Boolean = false
    private lateinit var compactionTask: CompactionTask

    override fun initialize(storeManager: StoreManager,) {
        this.compactionTask = CompactionTask(storeManager, this.storeConfig.mergeStrategy)
        val timeBetweenExecutionsInMillis = this.storeConfig.mergeIntervalMillis
        if (timeBetweenExecutionsInMillis > 0) {
            this.taskManager.scheduleRecurringWithTimeBetweenExecutions(compactionTask, timeBetweenExecutionsInMillis.milliseconds)
        } else {
            log.warn { "Compaction is disabled, because the merge interval is <= 0!" }
        }
    }

    override fun mergeNow(major: Boolean) {
        check(this.initialized){ "MergeService has not yet been initialized!" }
        if(major){
            this.compactionTask.runMajor(TaskMonitor.create())
        }else{
            this.compactionTask.run(TaskMonitor.create())
        }
    }

    override fun handleInMemoryInsertEvent(event: InMemoryLsmInsertEvent) {
        check(this.initialized){ "MergeService has not yet been initialized!" }
        if (event.lsmTree.inMemorySizeBytes < this.storeConfig.maxInMemoryTreeSizeInBytes) {
            return
        }
        // schedule a flush
        this.taskManager.executeAsync(FlushInMemoryTreeToDiskTask(event.lsmTree, this.storeConfig.maxInMemoryTreeSizeInBytes))
    }

    override fun handleCursorClosedEvent(lsmCursorClosedEvent: LsmCursorClosedEvent) {
        check(this.initialized){ "MergeService has not yet been initialized!" }
    }


}