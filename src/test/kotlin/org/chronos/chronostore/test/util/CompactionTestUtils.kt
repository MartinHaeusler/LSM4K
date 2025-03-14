package org.chronos.chronostore.test.util

import org.chronos.chronostore.api.compaction.LeveledCompactionStrategy
import org.chronos.chronostore.api.compaction.TieredCompactionStrategy
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.compaction.algorithms.LeveledCompactionProcess
import org.chronos.chronostore.lsm.compaction.algorithms.TieredCompactionProcess
import org.chronos.chronostore.lsm.compaction.model.CompactableStore
import org.chronos.chronostore.manifest.ManifestFile

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