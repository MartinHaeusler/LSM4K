package org.chronos.chronostore.lsm.compaction.strategy

import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.async.executor.AsyncTaskManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.forEachWithMonitor
import org.chronos.chronostore.lsm.compaction.tasks.CompactionTask
import org.chronos.chronostore.lsm.compaction.tasks.FlushInMemoryTreeToDiskTask
import org.chronos.chronostore.manifest.ManifestFile

class MergeServiceImpl(
    private val taskManager: AsyncTaskManager,
    private val storeConfig: ChronoStoreConfiguration,
    private val manifestFile: ManifestFile,
    private val storeManager: StoreManager,
) : MergeService {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    private val compactionTask = CompactionTask(
        this.storeManager,
        this.manifestFile,
    )

    init {
        val timeBetweenExecutions = this.storeConfig.compactionInterval
        if (timeBetweenExecutions != null && timeBetweenExecutions.isPositive()) {
            this.taskManager.scheduleRecurringWithTimeBetweenExecutions(compactionTask, timeBetweenExecutions)
        } else {
            warnAboutCompactionDisabled()
        }

    }

    override fun performMajorCompaction(taskMonitor: TaskMonitor) {
        this.compactionTask.runMajor(taskMonitor)
    }

    override fun performMinorCompaction(taskMonitor: TaskMonitor) {
        this.compactionTask.runMinor(taskMonitor)
    }

    override fun flushAllInMemoryStoresToDisk(taskMonitor: TaskMonitor) {
        taskMonitor.forEachWithMonitor(1.0, "Flushing In-Memory segments of LSM Trees", this.storeManager.getAllLsmTrees()) { subTaskMonitor, lsmTree ->
            val task = FlushInMemoryTreeToDiskTask(lsmTree, this.manifestFile)
            task.run(subTaskMonitor)
        }
        taskMonitor.reportDone()
    }

    private fun warnAboutCompactionDisabled() {
        log.warn {
            "Compaction is disabled, because the merge interval is NULL or negative!" +
                " You need to compact the store explicitly to prevent performance degradation."
        }
    }

}