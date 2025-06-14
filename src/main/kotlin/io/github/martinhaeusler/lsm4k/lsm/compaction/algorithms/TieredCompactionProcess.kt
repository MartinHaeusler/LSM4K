package io.github.martinhaeusler.lsm4k.lsm.compaction.algorithms

import io.github.martinhaeusler.lsm4k.api.compaction.TieredCompactionStrategy
import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor
import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor.Companion.mainTask
import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor.Companion.subTask
import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor.Companion.subTaskWithMonitor
import io.github.martinhaeusler.lsm4k.lsm.compaction.model.CompactableStore
import io.github.martinhaeusler.lsm4k.manifest.LSMFileInfo
import io.github.martinhaeusler.lsm4k.util.TierIndex

class TieredCompactionProcess(
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
                val fileSize = file?.sizeOnDisk?.bytes ?: 0L
                (sum ?: 0L) + fileSize
            }
    }


    fun runCompaction(monitor: TaskMonitor) = monitor.mainTask("Executing Tiered Compaction") {
        ensureExecutableOnlyOnce()
        // verify that we have enough files on disk to make the operation worth the effort.
        // If that's not the case, we exit out immediately.
        if (!areEnoughTiersOnDisk()) {
            monitor.reportDone()
            return
        }

        // choose the files we want to compact
        val (filesToCompact, compactionTrigger) = monitor.subTask(0.3, "Selecting Files to Compact") {
            this.selectFilesBasedOnCompactionTriggers()
        }
        if (filesToCompact.size <= 1 || compactionTrigger == null) {
            // nothing to do!
            return
        }

        val highestTierInCompaction = filesToCompact.maxOfOrNull { it.levelOrTier } ?: 0
        // we always merge "upwards", so the target tier is our current highest one +1
        // (there is one exception to the rule, that is if we reach the maximum allowed tiers according to
        // the store config. But in that case, we're guaranteed to merge into the highest tier)
        val outputTier = highestTierInCompaction + 1

        // squash the files together and write a new file to disk at the highest input tier
        monitor.subTaskWithMonitor(0.6) { mergeMonitor: TaskMonitor ->
            this.store.mergeFiles(
                fileIndices = filesToCompact.map { it.fileIndex }.toSet(),
                outputLevelOrTier = outputTier,
                keepTombstones = shouldKeepTombstonesInMerge(filesToCompact, outputTier),
                monitor = mergeMonitor,
                trigger = compactionTrigger,
            )
        }
    }

    private fun ensureExecutableOnlyOnce() {
        check(!this.executed) {
            "This compaction task has already been executed; please do not reuse task objects. Create a new one instead."
        }
        this.executed = true
    }

    private fun shouldKeepTombstonesInMerge(filesToCompact: List<LSMFileInfo>, outputTier: TierIndex): Boolean {
        // determine if we're merging to the highest tier (i.e. will there be LSM files "above" us after the merge?)
        val highestTierWithFiles = this.store.metadata.getHighestNonEmptyLevelOrTier() ?: 0


        // to we merge into the highest tier as the target?
        val mergesToHighestTier = outputTier >= highestTierWithFiles

        // if we merge into the highest tier, we DON'T want to keep the tombstones (they'll be useless).
        // if we DON'T merge into the highest tier, we WANT to keep the tombstones (they need to be propagated to higher tiers).
        return !mergesToHighestTier
    }


    private fun selectFilesBasedOnCompactionTriggers(): Pair<List<LSMFileInfo>, CompactionTrigger?> {
        // we have three triggers which we need to check in the given order:
        // - space-amplification-based compaction
        // - size-ratio-based compaction
        // - height-based compaction
        // So let's run them one by one, and see if any of them produce
        // a meaningful set of files to compact.

        val sizeAmplificationTriggerFiles = selectFilesForSpaceAmplificationTrigger()
        if (sizeAmplificationTriggerFiles.size > 1) {
            return Pair(sizeAmplificationTriggerFiles, CompactionTrigger.TIER_SPACE_AMPLIFICATION)
        }

        val sizeRatioTriggerFiles = selectFilesForSizeRatioTrigger()
        if (sizeRatioTriggerFiles.size > 1) {
            return Pair(sizeRatioTriggerFiles, CompactionTrigger.TIER_SIZE_RATIO)
        }

        val heightReductionTriggerFiles = selectFilesForHeightReductionTrigger()
        if (heightReductionTriggerFiles.size > 1) {
            return Pair(heightReductionTriggerFiles, CompactionTrigger.TIER_HEIGHT_REDUCTION)
        }

        // none of the triggers produced any outcome
        return Pair(emptyList(), null)
    }

    private fun areEnoughTiersOnDisk(): Boolean {
        // check how many tiers we have in the store
        val tier0Files = this.store.metadata.getFileIndicesAtTierOrLevel(0).size
        val nonEmptyTiers = this.store.metadata.getNumberOfNonEmptyTiers() - 1 + tier0Files

        // we don't have enough files on disk yet to make compaction worth the effort.
        return nonEmptyTiers >= this.configuration.numberOfTiers
    }

    private fun selectFilesForSpaceAmplificationTrigger(): List<LSMFileInfo> {
        val maxSizeAmplificationPercent = this.configuration.maxSpaceAmplificationPercent
        val highestTier = this.store.metadata.getHighestNonEmptyLevelOrTier()
            ?: return emptyList() // there are no tiers? Weird.

        val sizeOfLastTier = tierSizes[highestTier]?.takeIf { it != 0L }
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

        val tierSizes = this.tierSizes
        val acceptableSizeRatio = this.configuration.sizeRatio

        var sizeOfLowerTiers = 0.0

        var tiersInCompaction = 0
        for (tier in (0..highestTier)) {
            tiersInCompaction++
            val currentTierSize = tierSizes[tier]?.toDouble() ?: 0.0
            // compute our actual size ratio and check if it exceeds the configured limit
            val sizeRatioExceeded = if (sizeOfLowerTiers > 0.0) {
                val actualSizeRatio = currentTierSize / sizeOfLowerTiers
                actualSizeRatio > acceptableSizeRatio
            } else {
                // we're still on the first tier, avoid division by zero.
                // A single tier has no size ratio and therefore cannot exceed the limit.
                false
            }
            val enoughTiersSelected = tiersInCompaction >= minMergeTiers
            if (sizeRatioExceeded && enoughTiersSelected) {
                // compact all tiers above this one
                return this.getFilesInTiers(
                    minTier = 0,
                    maxTier = tier,
                )
            }
            // check the next-higher tier. To do that, we need
            // to accumulate the size of all lower ones, so add
            // our current tier size to the sum.
            sizeOfLowerTiers += currentTierSize
        }

        // no tier meets the criteria.
        return emptyList()
    }


    private fun selectFilesForHeightReductionTrigger(): List<LSMFileInfo> {
        val numberOfTiersInTree = this.store.metadata.getNumberOfNonEmptyTiers()

        val minMergeTiers = this.configuration.minMergeTiers
        if (numberOfTiersInTree < minMergeTiers) {
            // we don't have enough tiers yet
            return emptyList()
        }

        if (numberOfTiersInTree < this.configuration.numberOfTiers) {
            // the tree isn't "high" enough yet to warrant this operation.
            return emptyList()
        }

        // attempt to shrink the tree down to "number of tiers - 2". We use -2 as
        // a heuristic here to prevent having to do this operation again right away
        // on the next flush.
        val numberOfTiersToMerge = numberOfTiersInTree - this.configuration.numberOfTiers + 2
        if (numberOfTiersToMerge < this.configuration.minMergeTiers) {
            return emptyList()
        }

        return this.getFilesInTiers(
            minTier = 0,
            // maxTier is inclusive, so we need the -1 in the end
            maxTier = numberOfTiersToMerge - 1
        )
    }

    private fun selectFilesForTierZeroReductionTrigger(): List<LSMFileInfo> {
        val tierZeroFiles = this.store.metadata.getFileInfosAtTierOrLevel(0)
        if (tierZeroFiles.size <= 1) {
            return emptyList()
        }
        // compact ALL tier zero files
        return tierZeroFiles.toList()
    }


    private fun getFilesInTiers(
        minTier: TierIndex,
        maxTier: TierIndex,
    ): List<LSMFileInfo> {
        val metadata = this.store.metadata
        return (minTier..maxTier).flatMap {
            metadata.getFileInfosAtTierOrLevel(it)
        }
    }

}