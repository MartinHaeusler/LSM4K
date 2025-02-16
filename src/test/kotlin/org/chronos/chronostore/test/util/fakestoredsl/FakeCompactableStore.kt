package org.chronos.chronostore.test.util.fakestoredsl

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.merge.model.CompactableFile
import org.chronos.chronostore.lsm.merge.model.CompactableStore
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.util.FileIndex

class FakeCompactableStore(
    override val metadata: StoreMetadata,
    override val allFiles: List<CompactableFile>,
) : CompactableStore {

    val executedMerges = mutableListOf<ExecutedMerge>()

    override fun mergeFiles(fileIndices: Set<FileIndex>, keepTombstones: Boolean, monitor: TaskMonitor, updateManifest: (FileIndex) -> Unit) {
        this.executedMerges += ExecutedMerge(fileIndices.toSet(), keepTombstones)
    }

    class ExecutedMerge(
        val fileIndices: Set<FileIndex>,
        val keepTombstones: Boolean,
    )
}