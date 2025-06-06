package org.lsm4k.test.util

import org.lsm4k.api.compaction.LeveledCompactionStrategy
import org.lsm4k.api.compaction.TieredCompactionStrategy
import org.lsm4k.async.taskmonitor.TaskMonitor
import org.lsm4k.lsm.compaction.algorithms.LeveledCompactionProcess
import org.lsm4k.lsm.compaction.algorithms.TieredCompactionProcess
import org.lsm4k.lsm.compaction.model.CompactableStore

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