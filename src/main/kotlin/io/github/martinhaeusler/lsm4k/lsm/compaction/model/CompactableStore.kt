package io.github.martinhaeusler.lsm4k.lsm.compaction.model

import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor
import io.github.martinhaeusler.lsm4k.lsm.compaction.algorithms.CompactionTrigger
import io.github.martinhaeusler.lsm4k.manifest.StoreMetadata
import io.github.martinhaeusler.lsm4k.util.FileIndex
import io.github.martinhaeusler.lsm4k.util.LevelOrTierIndex
import io.github.martinhaeusler.lsm4k.util.StoreId

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