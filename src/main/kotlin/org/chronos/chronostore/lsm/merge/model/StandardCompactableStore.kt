package org.chronos.chronostore.lsm.merge.model

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.lsm.merge.algorithms.CompactionTrigger
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.StoreId

class StandardCompactableStore(
    val store: StoreImpl,
    override val metadata: StoreMetadata,
) : CompactableStore {

    companion object {

        private fun validateStoreIdMatchesMetadata(storeId: StoreId, metadataStoreId: StoreId) {
            require(storeId == metadataStoreId) {
                "StoreId (${storeId}) does not match the metadata StoreId (${metadataStoreId})!"
            }
        }

    }

    init {
        validateStoreIdMatchesMetadata(this.store.storeId, this.metadata.storeId)
    }

    override fun mergeFiles(
        fileIndices: Set<FileIndex>,
        keepTombstones: Boolean,
        trigger: CompactionTrigger,
        monitor: TaskMonitor,
        updateManifest: (FileIndex) -> Unit,
    ) {
        this.store.tree.mergeFiles(
            fileIndices = fileIndices,
            keepTombstones = keepTombstones,
            trigger = trigger,
            monitor = monitor,
            updateManifest = updateManifest,
        )
    }

    override val allFiles: List<CompactableFile>
        get() = this.store.tree.allFiles.map(::StandardCompactableFile)

}