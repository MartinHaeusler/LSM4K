package org.chronos.chronostore.lsm.merge.algorithms

import com.google.common.annotations.VisibleForTesting
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.api.compaction.TieredCompactionStrategy
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTask
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTaskWithMonitor
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.lsm.LSMTreeFile
import org.chronos.chronostore.manifest.LSMFileInfo
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.manifest.operations.TieredCompactionOperation
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.TierIndex
import org.chronos.chronostore.util.Timestamp

class TieredCompaction(
    val manifestFile: ManifestFile,
    val store: StoreImpl,
    val storeMetadata: StoreMetadata,
    val configuration: TieredCompactionStrategy,
    val monitor: TaskMonitor,
) {

    companion object {

        fun validateStoreIdsMatch(store: Store, storeMetadata: StoreMetadata) {
            require(store.storeId == storeMetadata.storeId) {
                "Store IDs of given parameters do not match: store.storeId = '${store.storeId}', storeMetadata.storeId = '${storeMetadata.storeId}'"
            }
        }

    }

    init {
        validateStoreIdsMatch(this.store, this.storeMetadata)
    }

    private val tierSizes: Map<TierIndex, Long> by lazy(LazyThreadSafetyMode.NONE) {
        val allFiles = this.store.tree.allFiles
        val indexToFile = allFiles.associateBy { it.index }

        this.storeMetadata.getAllFileInfos()
            .groupingBy { it.levelOrTier }
            .aggregate { _, sum, fileInfo, _ ->
                val file = indexToFile[fileInfo.fileIndex]
                val fileSize = file?.header?.sizeBytes ?: 0L
                (sum ?: 0L) + fileSize
            }
    }


    fun runCompaction() {
        // first, verify that we have enough files on disk to make the
        // operation worth the effort. If that's not the case, we exit
        // out immediately.
        if (!areEnoughTiersOnDisk()) {
            return
        }

        val now = System.currentTimeMillis()

        // choose the files we want to compact
        val filesToCompact = this.monitor.subTask(0.3, "Selecting Files to Compact") {
            this.selectFilesBasedOnCompactionTriggers(now)
        }
        if (filesToCompact.size <= 1) {
            // nothing to do!
            return
        }

        // squash the files together and write a new file to disk at the highest input tier
        this.monitor.subTaskWithMonitor(0.6) { mergeMonitor: TaskMonitor ->
            this.store.tree.mergeFiles(
                fileIndices = filesToCompact.map { it.fileIndex }.toSet(),
                keepTombstones = shouldKeepTombstonesInMerge(filesToCompact),
                monitor = mergeMonitor,
                // TODO [LOGIC] This is quite dirty. Can we find a nicer solution instead of the lambda?
                updateManifest = { outputFileIndex ->
                    val tierToFileIndices = mutableMapOf<TierIndex, MutableSet<FileIndex>>()
                    for (fileToCompact in filesToCompact) {
                        tierToFileIndices.getOrPut(fileToCompact.levelOrTier, ::mutableSetOf) += fileToCompact.fileIndex
                    }
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

    @VisibleForTesting
    fun shouldKeepTombstonesInMerge(filesToCompact: List<LSMFileInfo>): Boolean {
        // determine if we're merging to the highest tier (i.e. will there be LSM files "above" us after the merge?)
        val highestTierWithFiles = this.storeMetadata.getHighestNonEmptyLevelOrTier() ?: 0
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


    @VisibleForTesting
    fun selectFilesBasedOnCompactionTriggers(now: Timestamp): List<LSMFileInfo> {
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
        val nonEmptyTiers = storeMetadata.getNumberOfNonEmptyTiers()
        // we don't have enough files on disk yet to make compaction worth the effort.
        return nonEmptyTiers >= this.configuration.numberOfTiers
    }

    private fun selectFilesForDataAgeTrigger(now: Timestamp): List<LSMFileInfo> {
        val ageTolerance = this.configuration.ageTolerance.inWholeMilliseconds
        val tooOldFiles = this.store.tree.allFiles
            .filter { it.age(now) > ageTolerance }
            .sortedByDescending { it.age(now) }
        if (tooOldFiles.isEmpty()) {
            // no files are too old.
            return emptyList()
        }

        for (fileToCompact in tooOldFiles) {
            // determine in which tier this file is located
            val tier = this.storeMetadata.getTierForFile(fileToCompact.index)
            val nextHigherTier = this.storeMetadata.getNextHigherNonEmptyTier(tier)
                ?: continue // there are no files to merge with. Most likely, this file resides at the maximum tier.

            val filesToCompact = mutableListOf<LSMFileInfo>()
            filesToCompact += this.storeMetadata.getFileInfosAtTierOrLevel(tier)
            filesToCompact += this.storeMetadata.getFileInfosAtTierOrLevel(nextHigherTier)

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
        val highestTier = this.storeMetadata.getHighestNonEmptyLevelOrTier()
            ?: return emptyList() // there are no tiers? Weird.

        val sizeOfLastTier = tierSizes[highestTier]
            ?: return emptyList() // we were unable to determine the size of the highest tier? Weird.

        val sizeOfAllTiersExceptLast = tierSizes.asSequence().filter { it.key != highestTier }.sumOf { it.value }

        val ratio = sizeOfAllTiersExceptLast / sizeOfLastTier
        if (ratio < maxSizeAmplificationPercent) {
            // size amplification reduction cannot be achieved at this point in time
            // or would be insignificant.
            return emptyList()
        }

        // compact ALL the tiers (this is actually a major compaction)
        return storeMetadata.getAllFileInfos()
    }

    private fun selectFilesForSizeRatioTrigger(): List<LSMFileInfo> {
        val highestTier = this.storeMetadata.getHighestNonEmptyLevelOrTier()
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
                return (0..tier).flatMap { this.storeMetadata.getFileInfosAtTierOrLevel(it) }
            }
        }

        // no tier meets the criteria.
        return emptyList()
    }

    private fun LSMTreeFile.age(now: Timestamp): Long {
        val createdAt = this.header.metaData.createdAt
        return (now - createdAt).coerceAtLeast(0)
    }
}