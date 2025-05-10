package org.chronos.chronostore.lsm.compaction.model

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.compaction.algorithms.CompactionTrigger
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.LevelOrTierIndex
import org.chronos.chronostore.util.StoreId

interface CompactableStore {

    fun mergeFiles(
        fileIndices: Set<FileIndex>,
        keepTombstones: Boolean,
        trigger: CompactionTrigger,
        outputLevelOrTier: LevelOrTierIndex,
        monitor: TaskMonitor,
    )

    val storeId: StoreId
        get() = this.metadata.storeId

    val metadata: StoreMetadata

    val allFiles: List<CompactableFile>

}