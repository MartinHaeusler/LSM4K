package org.lsm4k.lsm.compaction.model

import org.lsm4k.async.taskmonitor.TaskMonitor
import org.lsm4k.lsm.compaction.algorithms.CompactionTrigger
import org.lsm4k.manifest.StoreMetadata
import org.lsm4k.util.FileIndex
import org.lsm4k.util.LevelOrTierIndex
import org.lsm4k.util.StoreId

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