package org.chronos.chronostore.lsm.merge.algorithms

import org.chronos.chronostore.api.compaction.LeveledCompactionStrategy
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.merge.model.CompactableFile
import org.chronos.chronostore.lsm.merge.model.CompactableStore
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.unit.BinarySize
import org.chronos.chronostore.util.unit.BinarySize.Companion.Bytes
import org.chronos.chronostore.util.unit.BinarySize.Companion.sumOf

class LeveledCompactionTask(
    val manifestFile: ManifestFile,
    val configuration: LeveledCompactionStrategy,
    val store: CompactableStore,
) {

    fun runCompaction(monitor: TaskMonitor) {

    }

    private fun computeTargetLevelSizes(): List<BinarySize> {
        // create a list with one entry per level, initialized with 0 bytes each.
        val targetLevelSizes = MutableList(this.configuration.maxLevels) {
            0.Bytes
        }
        var highestLevel = this.configuration.maxLevels
        val manifest = this.manifestFile.getManifest()
        val storeMetadata = this.store.metadata

        val realLevelSizes = getOnDiskSizesOfLevels(storeMetadata)

        val highestLevelSize = configuration.baseLevelMinSize


    }

    private fun getOnDiskSizesOfLevels(storeMetadata: StoreMetadata): List<BinarySize> {
        val fileIndexToFile = this.store.allFiles.associateBy { it.index }
        return (0..<this.configuration.maxLevels).map { levelIndex ->
            storeMetadata.getFileInfosAtTierOrLevel(levelIndex)
                .map { it.fileIndex }
                .mapNotNull { fileIndexToFile[it] }
                .sumOf { it.sizeOnDisk }
        }
    }

}