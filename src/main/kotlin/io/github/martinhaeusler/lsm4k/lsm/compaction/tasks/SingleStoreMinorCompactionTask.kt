package io.github.martinhaeusler.lsm4k.lsm.compaction.tasks

import io.github.martinhaeusler.lsm4k.api.compaction.LeveledCompactionStrategy
import io.github.martinhaeusler.lsm4k.api.compaction.TieredCompactionStrategy
import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor
import io.github.martinhaeusler.lsm4k.async.tasks.AsyncTask
import io.github.martinhaeusler.lsm4k.impl.Killswitch
import io.github.martinhaeusler.lsm4k.lsm.LSMTree
import io.github.martinhaeusler.lsm4k.lsm.compaction.algorithms.LeveledCompactionProcess
import io.github.martinhaeusler.lsm4k.lsm.compaction.algorithms.TieredCompactionProcess
import io.github.martinhaeusler.lsm4k.lsm.compaction.model.StandardCompactableStore

class SingleStoreMinorCompactionTask(
    private val lsmTree: LSMTree,
    private val killswitch: Killswitch,
) : AsyncTask {

    override val name: String
        get() = "Minor Compaction of '${this.lsmTree.storeId}'"

    override fun run(monitor: TaskMonitor) {
        try {
            when (val compactionStrategy = this.lsmTree.getStoreMetadata().compactionStrategy) {
                is LeveledCompactionStrategy -> this.performMinorLeveledCompaction(compactionStrategy, monitor)
                is TieredCompactionStrategy -> this.performMinorTieredCompaction(compactionStrategy, monitor)
            }
        } catch (t: Throwable) {
            killswitch.panic("An unexpected error occurred during Minor Compaction of store '${this.lsmTree.storeId}': ${t}", t)
            throw t
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