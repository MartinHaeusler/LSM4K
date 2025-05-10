package org.chronos.chronostore.test.util.fakestoredsl

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.compaction.algorithms.CompactionTrigger
import org.chronos.chronostore.lsm.compaction.model.CompactableFile
import org.chronos.chronostore.lsm.compaction.model.CompactableStore
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.LevelOrTierIndex

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