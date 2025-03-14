package org.chronos.chronostore.lsm.compaction.model

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.lsm.compaction.algorithms.CompactionTrigger
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.StoreId

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
        monitor: TaskMonitor,
    ) {
        this.tree.mergeFiles(
            fileIndices = fileIndices,
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