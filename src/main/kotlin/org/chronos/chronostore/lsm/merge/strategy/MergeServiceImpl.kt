package org.chronos.chronostore.lsm.merge.strategy

import mu.KotlinLogging
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.async.executor.AsyncTaskManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.forEachWithMonitor
import org.chronos.chronostore.lsm.event.InMemoryLsmFlushEvent
import org.chronos.chronostore.lsm.event.InMemoryLsmInsertEvent
import org.chronos.chronostore.lsm.event.LsmCursorClosedEvent
import org.chronos.chronostore.lsm.garbagecollector.tasks.GarbageCollectorTask
import org.chronos.chronostore.lsm.merge.tasks.CompactionTask
import org.chronos.chronostore.lsm.merge.tasks.FlushInMemoryTreeToDiskTask
import org.chronos.chronostore.lsm.merge.tasks.WALCompactionTask
import org.chronos.chronostore.util.unit.Bytes
import org.chronos.chronostore.wal.WriteAheadLog
import org.chronos.chronostore.wal2.WriteAheadLog2
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
    private lateinit var writeAheadLog: WriteAheadLog2
    private lateinit var walCompactionTask: WALCompactionTask
    private lateinit var garbageCollectorTask: GarbageCollectorTask
    private lateinit var storeManager: StoreManager

    override fun initialize(storeManager: StoreManager, writeAheadLog: WriteAheadLog2) {
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

        this.walCompactionTask = WALCompactionTask(this.writeAheadLog, storeManager)
        val walCompactionTimeOfDay = this.storeConfig.writeAheadLogCompactionTimeOfDay
        if (walCompactionTimeOfDay != null) {
            val startDelay = walCompactionTimeOfDay.nextOccurrence
            this.taskManager.scheduleRecurringWithFixedRate(this.walCompactionTask, startDelay.milliseconds, 24.hours)
        }

        this.garbageCollectorTask = GarbageCollectorTask(storeManager)
        val garbageCollectionTimeOfDay = this.storeConfig.garbageCollectionTimeOfDay
        if (garbageCollectionTimeOfDay != null) {
            val startDelay = garbageCollectionTimeOfDay.nextOccurrence
            this.taskManager.scheduleRecurringWithFixedRate(this.walCompactionTask, startDelay.milliseconds, 24.hours)
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

    override fun handleInMemoryInsertEvent(event: InMemoryLsmInsertEvent) {
        check(this.initialized) { "MergeService has not yet been initialized!" }
//        val oldFillRate = event.inMemorySizeBefore.bytes / this.storeConfig.maxInMemoryTreeSize.bytes.toDouble()
//        val newFillRate = event.inMemorySizeAfter.bytes / this.storeConfig.maxInMemoryTreeSize.bytes.toDouble()
//
//        // when we "cross" the 60% fill rate, we schedule a new flush task.
//        // Important: we *could* schedule a flush whenever we are above the fill rate threshold. However,
//        // this can create an entire wave of flush tasks hitting the scheduler, and we only really need to
//        // run one of them. In case that the tree is still too big after the flush due to concurrent inserts,
//        // we schedule another flush after the first one (see: "handleInMemoryFlushEvent").
//        if (oldFillRate < FILL_RATE_FLUSH_THRESHOLD && newFillRate >= FILL_RATE_FLUSH_THRESHOLD) {
//            this.taskManager.executeAsync(FlushInMemoryTreeToDiskTask(event.lsmTree, this.storeConfig.maxInMemoryTreeSize))
//        }
    }

    override fun handleInMemoryFlushEvent(event: InMemoryLsmFlushEvent) {
        check(this.initialized) { "MergeService has not yet been initialized!" }
//        val newFillRate = event.lsmTree.inMemorySize.bytes / this.storeConfig.maxInMemoryTreeSize.bytes.toDouble()
//        if (newFillRate >= FILL_RATE_FLUSH_THRESHOLD) {
//            println("REFLUSH")
//            // a flush has occurred, but there's still too much data in the in-memory tree.
//            // Schedule another flush (this will be repeated indefinitely until the tree becomes smaller than the threshold).
//            this.taskManager.executeAsync(FlushInMemoryTreeToDiskTask(event.lsmTree, this.storeConfig.maxInMemoryTreeSize))
//        }else{
//            println("NO REFLUSH NECESSARY")
//        }
    }

    override fun flushAllInMemoryStoresToDisk(taskMonitor: TaskMonitor) {
        taskMonitor.forEachWithMonitor(1.0, "Flushing In-Memory segments of LSM Trees", this.storeManager.getAllLsmTrees()) { subTaskMonitor, lsmTree ->
            val task = FlushInMemoryTreeToDiskTask(lsmTree)
            task.run(subTaskMonitor)
        }
        taskMonitor.reportDone()
    }

    override fun handleCursorClosedEvent(lsmCursorClosedEvent: LsmCursorClosedEvent) {
        check(this.initialized) { "MergeService has not yet been initialized!" }
    }


}