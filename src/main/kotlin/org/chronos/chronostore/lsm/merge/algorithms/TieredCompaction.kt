package org.chronos.chronostore.lsm.merge.algorithms

import org.chronos.chronostore.api.compaction.TieredCompactionStrategy
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTask
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTaskWithMonitor
import org.chronos.chronostore.lsm.merge.model.CompactableStore
import org.chronos.chronostore.manifest.LSMFileInfo
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.manifest.operations.TieredCompactionOperation
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.GroupingExtensions.toSets
import org.chronos.chronostore.util.TierIndex
import org.chronos.chronostore.util.Timestamp
import java.io.File

class TieredCompaction(
    val manifestFile: ManifestFile,
    val configuration: TieredCompactionStrategy,
    val store: CompactableStore,
) {

    private var executed: Boolean = false

    private val tierSizes: Map<TierIndex, Long> by lazy(LazyThreadSafetyMode.NONE) {
        val allFiles = this.store.allFiles
        val indexToFile = allFiles.associateBy { it.index }

        this.store.metadata.getAllFileInfos()
            .groupingBy { it.levelOrTier }
            .aggregate { _, sum, fileInfo, _ ->
                val file = indexToFile[fileInfo.fileIndex]
                val fileSize = file?.metadata?.sizeBytes ?: 0L
                (sum ?: 0L) + fileSize
            }
    }


    fun runCompaction(monitor: TaskMonitor, now: Timestamp = System.currentTimeMillis()) {
        ensureExecutableOnlyOnce()
        // verify that we have enough files on disk to make the operation worth the effort.
        // If that's not the case, we exit out immediately.
        if (!areEnoughTiersOnDisk()) {
            return
        }

        // choose the files we want to compact
        val filesToCompact = monitor.subTask(0.3, "Selecting Files to Compact") {
            this.selectFilesBasedOnCompactionTriggers(now)
        }
        if (filesToCompact.size <= 1) {
            // nothing to do!
            return
        }

        // squash the files together and write a new file to disk at the highest input tier
        monitor.subTaskWithMonitor(0.6) { mergeMonitor: TaskMonitor ->
            this.store.mergeFiles(
                fileIndices = filesToCompact.map { it.fileIndex }.toSet(),
                keepTombstones = shouldKeepTombstonesInMerge(filesToCompact),
                monitor = mergeMonitor,
                // TODO [LOGIC] This is quite dirty. Can we find a nicer solution instead of the lambda?
                updateManifest = { outputFileIndex ->
                    val tierToFileIndices = filesToCompact
                        .groupingBy { it.levelOrTier }
                        .toSets { it.fileIndex }

                    this.manifestFile.appendOperation { sequenceNumber ->
                        TieredCompactionOperation(
                            sequenceNumber = sequenceNumber,
                            storeId = this.store.storeId,
                            outputFileIndices = setOf(outputFileIndex),
                            tierToFileIndices = tierToFileIndices,
                        )
                    }
                }
            )
        }
    }

    private fun ensureExecutableOnlyOnce() {
        check(!this.executed) {
            "This compaction task has already been executed; please do not reuse task objects. Create a new one instead."
        }
        this.executed = true
    }

    private fun shouldKeepTombstonesInMerge(filesToCompact: List<LSMFileInfo>): Boolean {
        // determine if we're merging to the highest tier (i.e. will there be LSM files "above" us after the merge?)
        val highestTierWithFiles = this.store.metadata.getHighestNonEmptyLevelOrTier() ?: 0
        val highestTierInCompaction = filesToCompact.maxOfOrNull { it.levelOrTier } ?: 0
        // we always merge "upwards", so the target tier is our current highest one +1
        // (there is one exception to the rule, that is if we reach the maximum allowed tiers according to
        // the store config. But in that case, we're guaranteed to merge into the highest tier)
        val outputTier = highestTierInCompaction + 1

        // to we merge into the highest tier as the target?
        val mergesToHighestTier = outputTier >= highestTierWithFiles

        // if we merge into the highest tier, we DON'T want to keep the tombstones (they'll be useless).
        // if we DON'T merge into the highest tier, we WANT to keep the tombstones (they need to be propagated to higher tiers).
        return !mergesToHighestTier
    }


    private fun selectFilesBasedOnCompactionTriggers(now: Timestamp): List<LSMFileInfo> {
        // we have three triggers which we need to check in the given order:
        // - age-based compaction
        // - space-amplification-based compaction
        // - size-ratio-based compaction
        // So let's run them one by one, and see if any of them produce
        // a meaningful set of files to compact.
        return selectFilesForDataAgeTrigger(now).takeIf { it.size > 1 }
            ?: selectFilesForSizeAmplificationTrigger().takeIf { it.size > 1 }
            ?: selectFilesForSizeRatioTrigger().takeIf { it.size > 1 }
            // none of the triggers produced any outcome
            ?: emptyList()
    }

    private fun areEnoughTiersOnDisk(): Boolean {
        // check how many tiers we have in the store
        val nonEmptyTiers = this.store.metadata.getNumberOfNonEmptyTiers()
        // we don't have enough files on disk yet to make compaction worth the effort.
        return nonEmptyTiers >= this.configuration.numberOfTiers
    }

    private fun selectFilesForDataAgeTrigger(now: Timestamp): List<LSMFileInfo> {
        val ageTolerance = this.configuration.ageTolerance.inWholeMilliseconds
        val tooOldFiles = this.store.allFiles
            .filter { it.age(now) > ageTolerance }
            .sortedByDescending { it.age(now) }
        if (tooOldFiles.isEmpty()) {
            // no files are too old.
            return emptyList()
        }

        val storeMetadata = this.store.metadata

        for (fileToCompact in tooOldFiles) {
            // determine in which tier this file is located
            val tier = storeMetadata.getTierForFile(fileToCompact.index)
            val nextHigherTier = storeMetadata.getNextHigherNonEmptyTier(tier)
                ?: continue // there are no files to merge with. Most likely, this file resides at the maximum tier.

            val filesToCompact = mutableListOf<LSMFileInfo>()
            filesToCompact += storeMetadata.getFileInfosAtTierOrLevel(tier)
            filesToCompact += storeMetadata.getFileInfosAtTierOrLevel(nextHigherTier)

            if (filesToCompact.size > 1) {
                return filesToCompact
            }
        }

        // the only files that are too old reside in the highest
        // tier, so there's nowhere for them to go.
        return emptyList()
    }

    private fun selectFilesForSizeAmplificationTrigger(): List<LSMFileInfo> {
        val maxSizeAmplificationPercent = this.configuration.maxSizeAmplificationPercent
        val highestTier = this.store.metadata.getHighestNonEmptyLevelOrTier()
            ?: return emptyList() // there are no tiers? Weird.

        val sizeOfLastTier = tierSizes[highestTier]
            ?: return emptyList() // we were unable to determine the size of the highest tier? Weird.

        val sizeOfAllTiersExceptLast = tierSizes.asSequence()
            .filter { it.key != highestTier }
            .sumOf { it.value }

        val ratio = sizeOfAllTiersExceptLast / sizeOfLastTier
        if (ratio < maxSizeAmplificationPercent) {
            // size amplification reduction cannot be achieved at this point in time
            // or would be insignificant.
            return emptyList()
        }

        // compact ALL the tiers (this is actually a major compaction)
        return store.metadata.getAllFileInfos()
    }

    private fun selectFilesForSizeRatioTrigger(): List<LSMFileInfo> {
        val highestTier = this.store.metadata.getHighestNonEmptyLevelOrTier()
            ?: return emptyList() // there are no tiers? Weird.

        val minMergeTiers = this.configuration.minMergeTiers
        if (highestTier < minMergeTiers) {
            // we don't have enough tiers yet
            return emptyList()
        }

        for (tier in (minMergeTiers..<highestTier).reversed()) {
            val sizeOfThisTier = this.tierSizes[tier]
                ?: continue // we don't know the size of this tier... it's probably empty, ignore it.

            val sizeOfLowerTiers = (0..<tier).sumOf { this.tierSizes[it] ?: 0 }
            val ratio = (sizeOfLowerTiers / sizeOfThisTier.toDouble())
            if (ratio >= 1.0 + this.configuration.sizeRatio) {
                // compact all files below this tier into this tier
                return (0..tier).flatMap { this.store.metadata.getFileInfosAtTierOrLevel(it) }
            }
        }

        // no tier meets the criteria.
        return emptyList()
    }

}