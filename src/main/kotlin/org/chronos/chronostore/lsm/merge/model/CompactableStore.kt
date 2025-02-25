package org.chronos.chronostore.lsm.merge.model

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.merge.algorithms.CompactionTrigger
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.StoreId

interface CompactableStore {

    fun mergeFiles(
        fileIndices: Set<FileIndex>,
        keepTombstones: Boolean,
        trigger: CompactionTrigger,
        monitor: TaskMonitor,
        updateManifest: (FileIndex) -> Unit,
    )

    val storeId: StoreId
        get() = this.metadata.storeId

    val metadata: StoreMetadata

    val allFiles: List<CompactableFile>

}