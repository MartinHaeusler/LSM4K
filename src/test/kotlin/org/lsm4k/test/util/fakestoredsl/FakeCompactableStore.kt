package org.lsm4k.test.util.fakestoredsl

import org.lsm4k.async.taskmonitor.TaskMonitor
import org.lsm4k.lsm.compaction.algorithms.CompactionTrigger
import org.lsm4k.lsm.compaction.model.CompactableFile
import org.lsm4k.lsm.compaction.model.CompactableStore
import org.lsm4k.manifest.StoreMetadata
import org.lsm4k.util.FileIndex
import org.lsm4k.util.LevelOrTierIndex

class FakeCompactableStore(
    override val metadata: StoreMetadata,
    override val allFiles: List<CompactableFile>,
) : CompactableStore {

    val executedMerges = mutableListOf<ExecutedMerge>()

    override fun mergeFiles(fileIndices: Set<FileIndex>, keepTombstones: Boolean, trigger: CompactionTrigger, outputLevelOrTier: LevelOrTierIndex, monitor: TaskMonitor) {
        this.executedMerges += ExecutedMerge(
            fileIndices = fileIndices.toSet(),
            keepTombstones = keepTombstones,
            outputLevelOrTier = outputLevelOrTier,
            trigger = trigger,
        )
    }

    class ExecutedMerge(
        val fileIndices: Set<FileIndex>,
        val keepTombstones: Boolean,
        val outputLevelOrTier: LevelOrTierIndex,
        val trigger: CompactionTrigger,
    )
}