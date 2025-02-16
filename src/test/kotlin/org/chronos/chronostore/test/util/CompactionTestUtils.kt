package org.chronos.chronostore.test.util

import org.chronos.chronostore.api.compaction.TieredCompactionStrategy
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.merge.algorithms.TieredCompaction
import org.chronos.chronostore.lsm.merge.model.CompactableStore
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.util.Timestamp

object CompactionTestUtils {

    fun CompactableStore.executeTieredCompactionSynchronously(
        manifestFile: ManifestFile,
        strategy: TieredCompactionStrategy = TieredCompactionStrategy(),
        now: Timestamp = System.currentTimeMillis(),
    ) {
        val compaction = TieredCompaction(
            manifestFile = manifestFile,
            configuration = strategy,
            store = this
        )

        compaction.runCompaction(TaskMonitor.create(), now)
    }


}