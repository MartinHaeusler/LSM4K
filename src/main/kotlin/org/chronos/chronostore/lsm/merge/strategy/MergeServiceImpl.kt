package org.chronos.chronostore.lsm.merge.strategy

import mu.KotlinLogging
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.async.executor.AsyncTaskManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.forEachWithMonitor
import org.chronos.chronostore.lsm.garbagecollector.tasks.GarbageCollectorTask
import org.chronos.chronostore.lsm.merge.tasks.CompactionTask
import org.chronos.chronostore.lsm.merge.tasks.FlushInMemoryTreeToDiskTask
import org.chronos.chronostore.lsm.merge.tasks.WALShorteningTask
import org.chronos.chronostore.wal.WriteAheadLog
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
    private lateinit var walShorteningTask: WALShorteningTask
    private lateinit var garbageCollectorTask: GarbageCollectorTask
    private lateinit var storeManager: StoreManager

    override fun initialize(storeManager: StoreManager, writeAheadLog: WriteAheadLog) {
        this.writeAheadLog = writeAheadLog
        this.compactionTask = CompactionTask(storeManager, this.storeConfig.mergeStrategy)
        val timeBetweenExecutions = this.storeConfig.mergeInterval
        if (timeBetweenExecutions != null && timeBetweenExecutions.isPositive()) {
            this.taskManager.scheduleRecurringWithTimeBetweenExecutions(compactionTask, timeBetweenExecutions)
        } else {
            log.warn {
                "Compaction is disabled, because the merge interval is NULL or negative!" +
                    " You need to compact the store explicitly to prevent performance degradation."
            }
        }

        this.walShorteningTask = WALShorteningTask(this.writeAheadLog, storeManager)
        val walCompactionCron = this.storeConfig.writeAheadLogCompactionCron
        if (walCompactionCron != null) {
            this.taskManager.scheduleRecurringWithCron(this.walShorteningTask, walCompactionCron)
        }

        this.garbageCollectorTask = GarbageCollectorTask(storeManager)
        val garbageCollectionCron = this.storeConfig.garbageCollectionCron
        if (garbageCollectionCron != null) {
            this.taskManager.scheduleRecurringWithCron(this.garbageCollectorTask, garbageCollectionCron)
        }

        this.storeManager = storeManager

        this.initialized = true
    }

    override fun performMajorCompaction(taskMonitor: TaskMonitor) {
        check(this.initialized) { "MergeService has not yet been initialized!" }
        this.compactionTask.runMajor(taskMonitor)
    }

    override fun performMinorCompaction(taskMonitor: TaskMonitor) {
        check(this.initialized) { "MergeService has not yet been initialized!" }
        this.compactionTask.run(taskMonitor)
    }

    override fun flushAllInMemoryStoresToDisk(taskMonitor: TaskMonitor) {
        taskMonitor.forEachWithMonitor(1.0, "Flushing In-Memory segments of LSM Trees", this.storeManager.getAllLsmTrees()) { subTaskMonitor, lsmTree ->
            val task = FlushInMemoryTreeToDiskTask(lsmTree)
            task.run(subTaskMonitor)
        }
        taskMonitor.reportDone()
    }

}