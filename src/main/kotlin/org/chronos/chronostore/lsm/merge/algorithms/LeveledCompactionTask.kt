package org.chronos.chronostore.lsm.merge.algorithms

import org.chronos.chronostore.api.compaction.LeveledCompactionStrategy
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.merge.model.CompactableStore
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.manifest.operations.LeveledCompactionOperation
import org.chronos.chronostore.util.FileIndex
import kotlin.math.max

class LeveledCompactionTask(
    val manifestFile: ManifestFile,
    val configuration: LeveledCompactionStrategy,
    val store: CompactableStore,
) {

    fun runCompaction(monitor: TaskMonitor) {
        val storeMetadata = this.store.metadata
        val realLevelSizes = this.getOnDiskSizesOfLevels(storeMetadata)
        val targetLevelSizes = this.computeTargetLevelSizes(realLevelSizes)
        val minLevelWithTargetSize = targetLevelSizes.indexOfFirst { it > 0 }
        val level0FileIndicesToMerge = this.selectLevel0FilesToMerge(storeMetadata)
        if (level0FileIndicesToMerge.isNotEmpty()) {
            this.compactLevel0Files(level0FileIndicesToMerge, minLevelWithTargetSize)
            return
        }


    }

    private fun compactLevel0Files(filesToMerge: Set<FileIndex>, minLevelWithTargetSize: Int, monitor: TaskMonitor) {
        val overlappingSSTFilesInTargetLevel = findOverlappingSSTFiles(filesToMerge, minLevelWithTargetSize)

        this.store.mergeFiles(
            fileIndices = filesToMerge + overlappingSSTFilesInTargetLevel,
            keepTombstones = minLevelWithTargetSize == configuration.maxLevels,
            trigger = CompactionTrigger.LEVELED_LEVEL0,
            monitor = monitor
        ) { newFileIndex ->
            this.manifestFile.appendOperation { sequenceNumber ->
                LeveledCompactionOperation(
                    sequenceNumber = sequenceNumber,
                    storeId = this.store.storeId,
                    outputFileIndices = setOf(newFileIndex),
                    upperLevelIndex = minLevelWithTargetSize,
                    upperLevelFileIndices = overlappingSSTFilesInTargetLevel,
                    lowerLevelFileIndices = filesToMerge,
                    lowerLevelIndex = 0,
                )
            }
        }

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

    //
    //  [L0]:   [SST] [SST] [SST] [SST]         New files get added here. They SST contents are arbitrary. Not size constrained.
    //  [L1]:   [SST] [SST]                     Older files. Key ranges of SSTs are disjoint. Fraction of the size of L2.
    //  [L2]:   [SST] [SST]                     Even older files. Key ranges of SSTs are disjoint. Fraction of the size of L3.
    // ...
    //  [LB]:   [SST] [SST]                     "Base" level. Highest index. Oldest files. Key ranges of SSTs are disjoint.
    private fun computeTargetLevelSizes(realLevelSizes: Array<Long>): LongArray {
        // create a list with one entry per level, initialized with 0 bytes each.
        val targetLevelSizes = LongArray(this.configuration.maxLevels) { 0L }
        val highestLevel = this.configuration.maxLevels
        val baseLevelSize = configuration.baseLevelMinSize.bytes
        targetLevelSizes[highestLevel - 1] = max(baseLevelSize, realLevelSizes[highestLevel - 1])

        if (realLevelSizes[highestLevel - 1] < baseLevelSize) {
            // target level sizes are ok, tree isn't big enough yet
            return targetLevelSizes
        } else {
            // the target size of each level is a fraction of the higher level, given by target size multiplier.
            for (i in (0..<targetLevelSizes.lastIndex).reversed()) {
                val thisLevelSize = (targetLevelSizes[i + 1] / configuration.levelSizeMultiplier).toLong()
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

    private fun getOnDiskSizesOfLevels(storeMetadata: StoreMetadata): Array<Long> {
        val fileIndexToFile = this.store.allFiles.associateBy { it.index }
        return (0..<this.configuration.maxLevels).map { levelIndex ->
            storeMetadata.getFileInfosAtTierOrLevel(levelIndex)
                .map { it.fileIndex }
                .mapNotNull { fileIndexToFile[it] }
                .sumOf { it.sizeOnDisk.bytes }
        }.toTypedArray()
    }

}