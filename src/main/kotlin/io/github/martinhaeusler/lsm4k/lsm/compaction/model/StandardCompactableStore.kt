package io.github.martinhaeusler.lsm4k.lsm.compaction.model

import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor
import io.github.martinhaeusler.lsm4k.lsm.LSMTree
import io.github.martinhaeusler.lsm4k.lsm.compaction.algorithms.CompactionTrigger
import io.github.martinhaeusler.lsm4k.manifest.StoreMetadata
import io.github.martinhaeusler.lsm4k.util.FileIndex
import io.github.martinhaeusler.lsm4k.util.LevelOrTierIndex
import io.github.martinhaeusler.lsm4k.util.StoreId

class StandardCompactableStore(
    val tree: LSMTree,
) : CompactableStore {

    companion object {

        private fun validateStoreIdMatchesMetadata(storeId: StoreId, metadataStoreId: StoreId) {
            require(storeId == metadataStoreId) {
                "StoreId (${storeId}) does not match the metadata StoreId (${metadataStoreId})!"
            }
        }

    }

    init {
        validateStoreIdMatchesMetadata(this.tree.storeId, this.metadata.storeId)
    }

    override fun mergeFiles(
        fileIndices: Set<FileIndex>,
        keepTombstones: Boolean,
        trigger: CompactionTrigger,
        outputLevelOrTier: LevelOrTierIndex,
        monitor: TaskMonitor,
    ) {
        this.tree.mergeFiles(
            fileIndices = fileIndices,
            outputLevelOrTier = outputLevelOrTier,
            keepTombstones = keepTombstones,
            trigger = trigger,
            monitor = monitor,
        )
    }

    override val metadata: StoreMetadata
        get() = tree.getStoreMetadata()

    override val allFiles: List<CompactableFile>
        get() = this.tree.allFiles.map(::StandardCompactableFile)

}