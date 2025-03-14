package org.chronos.chronostore.lsm.compaction.tasks

import org.chronos.chronostore.api.compaction.LeveledCompactionStrategy
import org.chronos.chronostore.api.compaction.TieredCompactionStrategy
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.lsm.compaction.algorithms.LeveledCompactionProcess
import org.chronos.chronostore.lsm.compaction.algorithms.TieredCompactionProcess
import org.chronos.chronostore.lsm.compaction.model.StandardCompactableStore

class SingleStoreMinorCompactionTask(
    val lsmTree: LSMTree,
) : AsyncTask {

    override val name: String
        get() = "Minor Compaction of '${this.lsmTree.storeId}'"

    override fun run(monitor: TaskMonitor) {
        when (val compactionStrategy = this.lsmTree.getStoreMetadata().compactionStrategy) {
            is LeveledCompactionStrategy -> this.performMinorLeveledCompaction(compactionStrategy, monitor)
            is TieredCompactionStrategy -> this.performMinorTieredCompaction(compactionStrategy, monitor)
        }

    }

    private fun performMinorTieredCompaction(
        compactionStrategy: TieredCompactionStrategy,
        monitor: TaskMonitor,
    ) {
        val compactableStore = StandardCompactableStore(
            this.lsmTree,
        )

        val compaction = TieredCompactionProcess(
            store = compactableStore,
            configuration = compactionStrategy,
        )

        compaction.runCompaction(monitor)
    }

    private fun performMinorLeveledCompaction(
        compactionStrategy: LeveledCompactionStrategy,
        monitor: TaskMonitor,
    ) {
        val compactableStore = StandardCompactableStore(
            this.lsmTree,
        )

        val compaction = LeveledCompactionProcess(
            store = compactableStore,
            configuration = compactionStrategy,
        )

        compaction.runCompaction(monitor)
    }

}