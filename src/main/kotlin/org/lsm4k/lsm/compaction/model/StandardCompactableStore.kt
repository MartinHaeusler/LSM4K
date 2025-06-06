package org.lsm4k.lsm.compaction.model

import org.lsm4k.async.taskmonitor.TaskMonitor
import org.lsm4k.lsm.LSMTree
import org.lsm4k.lsm.compaction.algorithms.CompactionTrigger
import org.lsm4k.manifest.StoreMetadata
import org.lsm4k.util.FileIndex
import org.lsm4k.util.LevelOrTierIndex
import org.lsm4k.util.StoreId

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