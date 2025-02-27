package org.chronos.chronostore.test.util

import org.chronos.chronostore.api.compaction.LeveledCompactionStrategy
import org.chronos.chronostore.api.compaction.TieredCompactionStrategy
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.merge.algorithms.LeveledCompactionTask
import org.chronos.chronostore.lsm.merge.algorithms.TieredCompactionTask
import org.chronos.chronostore.lsm.merge.model.CompactableStore
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.util.Timestamp

object CompactionTestUtils {

    fun CompactableStore.executeTieredCompactionSynchronously(
        manifestFile: ManifestFile,
        strategy: TieredCompactionStrategy = TieredCompactionStrategy(),
    ) {
        val compaction = TieredCompactionTask(
            manifestFile = manifestFile,
            configuration = strategy,
            store = this,
        )

        compaction.runCompaction(TaskMonitor.create())
    }

    fun CompactableStore.executeLeveledCompactionSynchronously(
        manifestFile: ManifestFile,
        strategy: LeveledCompactionStrategy = LeveledCompactionStrategy(),
    ) {
        val compaction = LeveledCompactionTask(
            manifestFile = manifestFile,
            configuration = strategy,
            store = this,
        )

        compaction.runCompaction(TaskMonitor.create())
    }

}