package io.github.martinhaeusler.lsm4k.lsm.compaction.algorithms

import io.github.martinhaeusler.lsm4k.api.compaction.LeveledCompactionStrategy
import io.github.martinhaeusler.lsm4k.api.compaction.TieredCompactionStrategy
import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor
import io.github.martinhaeusler.lsm4k.lsm.compaction.model.CompactableStore
import io.github.martinhaeusler.lsm4k.manifest.ManifestFile

class FullCompactionTask(
    val manifestFile: ManifestFile,
    val store: CompactableStore,
) {

    fun runCompaction(monitor: TaskMonitor) {
        if (store.allFiles.isEmpty()) {
            // store is empty, nothing to compact
            return
        }

        val maxLevelOrTierIndex = when (val compactionStrategy = store.metadata.compactionStrategy) {
            is LeveledCompactionStrategy -> compactionStrategy.maxLevels - 1
            is TieredCompactionStrategy -> compactionStrategy.numberOfTiers - 1
        }

        val firstNonEmptyLevelOrTier = (0..maxLevelOrTierIndex).asSequence()
            .map { it to store.metadata.getFileIndicesAtTierOrLevel(it) }
            .first { it.second.isNotEmpty() }
            .first

        if (firstNonEmptyLevelOrTier >= maxLevelOrTierIndex) {
            // all data is already in the highest level or tier
            // -> nothing to do
            return
        }
        val fileIndices = store.metadata.getAllFileIndices()
        this.store.mergeFiles(
            fileIndices = fileIndices,
            outputLevelOrTier = maxLevelOrTierIndex,
            keepTombstones = false,
            trigger = CompactionTrigger.FULL_COMPACTION,
            monitor = monitor
        )
    }

}