package org.chronos.chronostore.lsm.merge.strategy

import mu.KotlinLogging
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.async.executor.AsyncTaskManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.forEachWithMonitor
import org.chronos.chronostore.lsm.event.InMemoryLsmInsertEvent
import org.chronos.chronostore.lsm.event.LsmCursorClosedEvent
import org.chronos.chronostore.lsm.merge.tasks.CompactionTask
import org.chronos.chronostore.lsm.merge.tasks.FlushInMemoryTreeToDiskTask
import org.chronos.chronostore.lsm.merge.tasks.WALCompactionTask
import org.chronos.chronostore.wal.WriteAheadLog
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.hours
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
    private lateinit var writeAheadLog: WriteAheadLog
    private lateinit var walCompactionTask: WALCompactionTask
    private lateinit var storeManager: StoreManager

    override fun initialize(storeManager: StoreManager, writeAheadLog: WriteAheadLog) {
        this.writeAheadLog = writeAheadLog
        this.compactionTask = CompactionTask(storeManager, this.storeConfig.mergeStrategy)
        val timeBetweenExecutionsInMillis = this.storeConfig.mergeIntervalMillis
        if (timeBetweenExecutionsInMillis > 0) {
            this.taskManager.scheduleRecurringWithTimeBetweenExecutions(compactionTask, timeBetweenExecutionsInMillis.milliseconds)
        } else {
            log.warn { "Compaction is disabled, because the merge interval is <= 0!" }
        }
        this.walCompactionTask = WALCompactionTask(this.writeAheadLog, storeManager)

        val walCompactionTimeOfDay = this.storeConfig.writeAheadLogCompactionTimeOfDay
        if (walCompactionTimeOfDay != null) {
            val timestampToday = walCompactionTimeOfDay.timestampToday
            val startTimestamp = if (timestampToday < System.currentTimeMillis()) {
                timestampToday + TimeUnit.HOURS.toMillis(24)
            } else {
                timestampToday
            }
            val startDelay = startTimestamp - System.currentTimeMillis()
            this.taskManager.scheduleRecurringWithFixedRate(this.walCompactionTask, startDelay.milliseconds, 24.hours)
        }

        this.storeManager = storeManager

        this.initialized = true
        // TODO: add cleanup task which calls LSMTree.performGarbageCollection
    }

    override fun mergeNow(major: Boolean, taskMonitor: TaskMonitor) {
        check(this.initialized) { "MergeService has not yet been initialized!" }
        if (major) {
            this.compactionTask.runMajor(taskMonitor)
        } else {
            this.compactionTask.run(taskMonitor)
        }
    }

    override fun handleInMemoryInsertEvent(event: InMemoryLsmInsertEvent) {
        check(this.initialized) { "MergeService has not yet been initialized!" }
        if (event.lsmTree.inMemorySizeBytes < this.storeConfig.maxInMemoryTreeSizeInBytes) {
            return
        }
        // schedule a flush
        this.taskManager.executeAsync(FlushInMemoryTreeToDiskTask(event.lsmTree, this.storeConfig.maxInMemoryTreeSizeInBytes))
    }

    override fun flushAllInMemoryStoresToDisk(taskMonitor: TaskMonitor) {
        taskMonitor.forEachWithMonitor(1.0, "Flushing In-Memory segments of LSM Trees", this.storeManager.getAllLsmTrees()) { subTaskMonitor, lsmTree ->
            val task = FlushInMemoryTreeToDiskTask(lsmTree, maxInMemoryTreeSizeInBytes = 0L)
            task.run(subTaskMonitor)
        }
        taskMonitor.reportDone()
    }

    override fun handleCursorClosedEvent(lsmCursorClosedEvent: LsmCursorClosedEvent) {
        check(this.initialized) { "MergeService has not yet been initialized!" }
    }


}