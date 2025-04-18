package org.chronos.chronostore.lsm.compaction.algorithms

import com.google.common.annotations.VisibleForTesting
import org.chronos.chronostore.api.compaction.LeveledCompactionStrategy
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.mainTask
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTask
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTaskWithMonitor
import org.chronos.chronostore.lsm.compaction.model.CompactableStore
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.LevelIndex
import org.chronos.chronostore.util.unit.BinarySize
import kotlin.math.max

class LeveledCompactionProcess(
    val configuration: LeveledCompactionStrategy,
    val store: CompactableStore,
) {

    companion object {

        @VisibleForTesting
        fun computeTargetLevelSizes(
            realLevelSizes: LongArray,
            maxLevels: Int,
            baseLevelMinSize: BinarySize,
            levelSizeMultiplier: Double,
        ): LongArray {
            //
            //  [L0]:   [SST] [SST] [SST] [SST]         New files get added here. They SST contents are arbitrary. Not size constrained.
            //  [L1]:   [SST] [SST]                     Older files. Key ranges of SSTs are disjoint. Fraction of the size of L2.
            //  [L2]:   [SST] [SST]                     Even older files. Key ranges of SSTs are disjoint. Fraction of the size of L3.
            // ...
            //  [LB]:   [SST] [SST]                     "Base" level. Highest index. Oldest files. Key ranges of SSTs are disjoint.
            //
            //
            // create a list with one entry per level, initialized with 0 bytes each.
            val targetLevelSizes = LongArray(maxLevels) { 0L }
            val baseLevelSize = baseLevelMinSize.bytes
            targetLevelSizes[maxLevels - 1] = max(baseLevelSize, realLevelSizes.last())

            if (realLevelSizes.last() < baseLevelSize) {
                // target level sizes are ok, tree isn't big enough yet
                return targetLevelSizes
            } else {
                // the target size of each level is a fraction of the higher level, given by target size multiplier.
                // exclude level 0, it has no target size (this is where new SST files are flushed into)
                for (i in (1..<targetLevelSizes.lastIndex).reversed()) {
                    val thisLevelSize = (targetLevelSizes[i + 1] / levelSizeMultiplier).toLong()
                    targetLevelSizes[i] = thisLevelSize
                    if (thisLevelSize < baseLevelSize) {
                        // at most one level is allowed to have a size
                        // smaller than the configured base level size,
                        // the lower levels have a target size of zero.
                        break
                    }
                }
                return targetLevelSizes
            }
        }


        @VisibleForTesting
        fun computeCurrentSizeToTargetSizeRatios(currentSizes: LongArray, targetSizes: LongArray): DoubleArray {
            require(currentSizes.size == targetSizes.size) {
                "Argument 'currentSizes' (size: ${currentSizes.size}) must have the same size as argument 'targetSizes' (size: ${targetSizes.size})!"
            }
            val ratios = DoubleArray(currentSizes.size) { 0.0 }
            for (i in ratios.indices) {
                val currentSize = currentSizes[i]
                val targetSize = targetSizes[i]
                if (targetSize <= 0) {
                    continue
                }
                val ratio = currentSize.toDouble() / targetSize.toDouble()
                if (ratio > 1.0) {
                    // only keep ratios which are greater than 1.0
                    ratios[i] = ratio
                } else {
                    // smaller ratios get treated as 0.0 (these levels are not "too big" yet and thus require no compaction)
                    ratios[i] = 0.0
                }
            }
            return ratios
        }

        @VisibleForTesting
        fun selectLevelWithHighestSizeToTargetRatio(levelSizeRatios: DoubleArray): LevelIndex? {
            if (levelSizeRatios.isEmpty()) {
                // safeguard, shouldn't happen.
                return null
            }
            var maxIndex = -1
            var maxValue = 0.0
            for (i in levelSizeRatios.indices) {
                val value = levelSizeRatios[i]
                if (value > maxValue) {
                    maxIndex = i
                    maxValue = value
                }
            }
            if (maxValue <= 0.0) {
                // none of the levels is larger than it's supposed to be.
                return null
            }
            return maxIndex
        }

    }

    fun runCompaction(monitor: TaskMonitor) = monitor.mainTask("Executing Leveled Compaction") {
        val storeMetadata = this.store.metadata
        val realLevelSizes = monitor.subTask(0.1, "Collecting current disk footprints") {
            this.getOnDiskSizesOfLevels(storeMetadata)
        }
        val targetLevelSizes = monitor.subTask(0.1, "Computing target level sizes") {
            computeTargetLevelSizes(
                realLevelSizes = realLevelSizes,
                maxLevels = this.configuration.maxLevels,
                baseLevelMinSize = this.configuration.baseLevelMinSize,
                levelSizeMultiplier = this.configuration.levelSizeMultiplier
            )
        }
        val minLevelWithTargetSize = targetLevelSizes.indexOfFirst { it > 0 }

        val highestNonEmptyLevel = realLevelSizes.indexOfLast { it > 0 }

        // trigger #1: Check if there are too many level 0 SSTs and compact them if necessary.
        val level0FileIndicesToMerge = monitor.subTask(0.1, "Checking if Level 0 compaction is necessary") {
            this.selectLevel0FilesToMerge(storeMetadata)
        }
        if (level0FileIndicesToMerge.isNotEmpty()) {
            monitor.subTaskWithMonitor(0.7) { subMonitor ->
                this.compactLevel0Files(level0FileIndicesToMerge, minLevelWithTargetSize, highestNonEmptyLevel, subMonitor)
            }
            return
        }

        // trigger #2: Check if any level of the tree exceeds its target size, and compact the worst offender.
        val levelWithHighestSizeToTargetRatio = monitor.subTask(0.1, "Checking Level Target Size Ratios") {
            val currentSizeToTargetSizeRatios = computeCurrentSizeToTargetSizeRatios(realLevelSizes, targetLevelSizes)
            selectLevelWithHighestSizeToTargetRatio(currentSizeToTargetSizeRatios)
        }

        if (levelWithHighestSizeToTargetRatio != null) {
            monitor.subTaskWithMonitor(0.6) { subMonitor ->
                compactBasedOnTargetSizeRatio(levelWithHighestSizeToTargetRatio, highestNonEmptyLevel, subMonitor)
            }
            return
        }
    }

    private fun compactBasedOnTargetSizeRatio(
        levelWithHighestSizeToTargetRatio: LevelIndex,
        highestNonEmptyLevel: LevelIndex,
        monitor: TaskMonitor,
    ) {
        val fileIndicesInLevel = this.store.metadata.getFileIndicesAtTierOrLevel(levelWithHighestSizeToTargetRatio)
        val fileIndexToFile = this.store.allFiles.associateBy { it.index }
        val comparator = this.configuration.fileSelectionStrategy.comparator

        val chosenSSTFile = fileIndicesInLevel.asSequence()
            .mapNotNull { fileIndexToFile[it] }
            .minWithOrNull(comparator)
            ?: return // just a safeguard

        val targetLevelIndex = levelWithHighestSizeToTargetRatio + 1
        val overlappingSSTFilesInTargetLevel = findOverlappingSSTFiles(setOf(chosenSSTFile.index), targetLevelIndex)
        this.store.mergeFiles(
            fileIndices = setOf(chosenSSTFile.index) + overlappingSSTFilesInTargetLevel,
            // if we do not merge to the highest non-empty level, we have to keep the tombstones during the merge.
            keepTombstones = targetLevelIndex != highestNonEmptyLevel,
            trigger = CompactionTrigger.LEVELED_TARGET_SIZE_RATIO,
            monitor = monitor
        )
    }

    private fun compactLevel0Files(filesToMerge: Set<FileIndex>, minLevelWithTargetSize: Int, highestNonEmptyLevel: Int, monitor: TaskMonitor) {
        val overlappingSSTFilesInTargetLevel = findOverlappingSSTFiles(filesToMerge, minLevelWithTargetSize)

        val keepTombstones = if (highestNonEmptyLevel == 0) {
            // if the highest non-empty level *is* zero, then there is no data at all in the higher ranks and we can drop
            // the tombstones immediately.
            false
        } else {
            // if we do not merge to the highest level, we have to keep the tombstones during the merge.
            minLevelWithTargetSize != configuration.maxLevels
        }

        this.store.mergeFiles(
            fileIndices = filesToMerge + overlappingSSTFilesInTargetLevel,
            keepTombstones = keepTombstones,
            trigger = CompactionTrigger.LEVELED_LEVEL0,
            monitor = monitor
        )
    }

    private fun findOverlappingSSTFiles(inputFileIndices: Set<FileIndex>, level: Int): Set<FileIndex> {
        val fileIndexToFile = this.store.allFiles.associateBy { it.index }
        val minKey = inputFileIndices.asSequence()
            .mapNotNull { fileIndexToFile[it] }
            .mapNotNull { it.metadata.minKey }
            .min()

        val maxKey = inputFileIndices.asSequence()
            .mapNotNull { fileIndexToFile[it] }
            .mapNotNull { it.metadata.maxKey }
            .max()

        val overlappingSSTFileIndices = this.store.metadata.getFileIndicesAtTierOrLevel(level).asSequence()
            .mapNotNull { fileIndexToFile[it] }
            .filter { it.metadata.overlaps(minKey, maxKey) }
            .map { it.index }
            .toSet()

        return overlappingSSTFileIndices
    }

    private fun selectLevel0FilesToMerge(storeMetadata: StoreMetadata): Set<FileIndex> {
        val level0FileInfos = storeMetadata.getFileInfosAtTierOrLevel(0)
        if (level0FileInfos.size < this.configuration.level0FileNumberCompactionTrigger) {
            // not enough files in level 0
            return emptySet()
        }
        return level0FileInfos.asSequence()
            .map { it.fileIndex }
            .toSet()
    }


    private fun getOnDiskSizesOfLevels(storeMetadata: StoreMetadata): LongArray {
        val fileIndexToFile = this.store.allFiles.associateBy { it.index }
        return (0..<this.configuration.maxLevels).map { levelIndex ->
            storeMetadata.getFileInfosAtTierOrLevel(levelIndex)
                .map { it.fileIndex }
                .mapNotNull { fileIndexToFile[it] }
                .sumOf { it.sizeOnDisk.bytes }
        }.toLongArray()
    }


}