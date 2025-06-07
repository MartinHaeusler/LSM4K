package io.github.martinhaeusler.lsm4k.test.util

import io.github.martinhaeusler.lsm4k.api.compaction.LeveledCompactionStrategy
import io.github.martinhaeusler.lsm4k.api.compaction.TieredCompactionStrategy
import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor
import io.github.martinhaeusler.lsm4k.lsm.compaction.algorithms.LeveledCompactionProcess
import io.github.martinhaeusler.lsm4k.lsm.compaction.algorithms.TieredCompactionProcess
import io.github.martinhaeusler.lsm4k.lsm.compaction.model.CompactableStore

object CompactionTestUtils {

    fun CompactableStore.executeTieredCompactionSynchronously(
        strategy: TieredCompactionStrategy = TieredCompactionStrategy(),
    ) {
        val compaction = TieredCompactionProcess(
            configuration = strategy,
            store = this,
        )

        compaction.runCompaction(TaskMonitor.create())
    }

    fun CompactableStore.executeLeveledCompactionSynchronously(
        strategy: LeveledCompactionStrategy = LeveledCompactionStrategy(),
    ) {
        val compaction = LeveledCompactionProcess(
            configuration = strategy,
            store = this,
        )

        compaction.runCompaction(TaskMonitor.create())
    }

}