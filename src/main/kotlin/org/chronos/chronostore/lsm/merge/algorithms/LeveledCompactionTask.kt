package org.chronos.chronostore.lsm.merge.algorithms

import org.chronos.chronostore.api.compaction.LeveledCompactionStrategy
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.merge.model.CompactableStore
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.util.Timestamp

class LeveledCompactionTask(
    val manifestFile: ManifestFile,
    val configuration: LeveledCompactionStrategy,
    val store: CompactableStore,
) {

    fun runCompaction(monitor: TaskMonitor, now: Timestamp = System.currentTimeMillis()) {

    }

}