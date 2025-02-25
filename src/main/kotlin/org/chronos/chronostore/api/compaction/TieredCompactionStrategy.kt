package org.chronos.chronostore.api.compaction

import org.chronos.chronostore.impl.annotations.PersistentClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days

/**
 * Tiered compaction uses tiers (instead of levels) and always compacts whole tiers.
 *
 * # Trade-Offs and Use Cases
 *
 * This strategy accepts a higher degree of read amplification in exchange for lower write
 * amplification. This allows for fewer write operations overall, which makes it suitable
 * for storage media where repeated overwrites can damage the hardware (e.g. Solid State Drives).
 * Also, due to the reduced write amplification, it allows for faster data ingestion rates.
 *
 * # Key Concepts
 *
 * The key concepts are:
 *
 * - **By default, every tier contains a single SST file.**
 *
 *    The only exception occurs when a single SST file gets too big, in which case it is split into several
 *    files with disjoint key ranges. Then all of these files will be treated to be in the same tier. This
 *    difference is of no consequence for the rest of the algorithm, because it only ever deals with "whole
 *    tiers".
 *
 * - **New files produced by flush tasks are always inserted at tier 0.**
 *
 *    Doing so moves all other files up by one tier.
 *
 * - **SSTs with newer data always have strictly lower tier numbers than SSTs with older data.**
 *
 *    The tiers are therefore sorted by data age from newest (tier 0) to oldest (highest tier).
 *
 * - **Compaction always merges 2 or more tiers into one, replacing the original tiers.**
 *
 * - **Compaction may only merge adjacent tiers, or consecutive blocks of tiers.**
 *
 *    It cannot happen that Tier 2 gets merged with Tier 4 while Tier 3 is left alone. Doing so
 *    would violate the principle that newer data lives in lower tiers, no matter if we place
 *    the compaction result before or after Tier 3 in the example.
 *
 * # Configuration
 *
 * There are multiple compaction triggers, but all of them are overruled by [numberOfTiers]. As
 * long as we don't have at least this many tiers, **no** compaction will happen (regardless of
 * the trigger).
 *
 * The triggers will be processed in order, and lower-priority triggers will only be checked if
 * the higher-priority ones did not produce a compaction:
 *
 *  1. **Data Age**
 *
 *     Highest priority trigger. Configured by [ageTolerance]. Compacts tiers with old data.
 *
 *  2. **Space Amplification**
 *
 *     Configured by [maxSizeAmplificationPercent]. Attempts to reduce the disk footprint by
 *     compacting files. Looks for the compaction with the most potential for size reduction.
 *
 *  3. **Size Ratio**
 *
 *     Configured by [sizeRatio] and [minMergeTiers]. Attempts to make all SSTs in all tiers
 *     approximately the same size on disk, eliminating very small SSTs in the process.
 *
 * In addition to these triggers, a **major compaction** can occur periodically which merges
 * all tiers into one.
 */
@PersistentClass(format = PersistentClass.Format.JSON, details = "Used in Manifest.")
class TieredCompactionStrategy(
    /**
     * Tiered Compaction will only trigger tasks when the number of tiers (sorted runs) is larger than this value.
     *
     * Otherwise, it does not trigger any compaction (no matter which of the other compaction triggers apply).
     */
    val numberOfTiers: Int = 8,

    /**
     * Configures the "Data Age Trigger" for compaction.
     *
     * Any files older than the age tolerance will become candidates for compaction. The compacted file will
     * then start out with "age zero" again.
     */
    val ageTolerance: Duration = 30.days,

    /**
     * Configures the "Space Amplification Trigger" for compaction.
     *
     * Gives a threshold (where 1.0 = 100%, 2.0 = 200% etc.) that states the tolerated disk space amplification.
     *
     * The amplification is relative to the (estimated) size of the stored data. When the given threshold is
     * exceeded, a compaction will be triggered, reducing the disk footprint of the store.
     *
     * Formula:
     *
     * ```
     * ratio = sum(size(all tiers except last)) / size(last tier)
     * ```
     *
     * A compaction will be triggered if `ratio` >= [maxSpaceAmplificationPercent].
     *
     * Lower ratios mean that the stores will be compacted more eagerly.
     */
    val maxSpaceAmplificationPercent: Double = 2.0,

    /**
     * Configures the "Size Ratio Trigger" for compaction.
     *
     * Gives a threshold (where 1.0 = 100%, 2.0 = 200%, etc.) that states how much larger higher tiers need
     * to be compared to lower tiers.
     *
     * Formula:
     *
     * ```
     * ratio = sum(size(all previous tiers) / size(this tier)
     * ```
     *
     * A compaction will be triggered if `ratio` >= `1.0 + sizeRatio`.
     *
     * Please note that this formula will be tested for EVERY tier individually. If the ratio is exceeded
     * by any tier, *all* lower tiers will be compacted into the current one. This happens only if there are
     * at least [minMergeTiers] are affected.
     */
    val sizeRatio: Double = 1.0,

    /**
     * Configures the "Size Ratio Trigger" for compaction.
     *
     * Size-Ratio-based compaction will only occur if at least [minMergeTiers] are affected.
     */
    val minMergeTiers: Int = 2,
) : CompactionStrategy {

    override fun toString(): String {
        return "TieredCompactionStrategy[tiers=${this.numberOfTiers}, sizeAmp=${String.format("%.2f", this.maxSpaceAmplificationPercent)}, sizeRatio=${String.format("%.2f", this.sizeRatio)}]"
    }

}