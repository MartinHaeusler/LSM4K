package org.lsm4k.api.compaction

import org.lsm4k.impl.annotations.PersistentClass

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
 *     Highest priority trigger. Newer data will have a higher priority for compaction.
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
    val numberOfTiers: Int = DEFAULT_NUMBER_OF_TIERS,

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
    val maxSpaceAmplificationPercent: Double = DEFAULT_MAX_SPACE_AMPLIFICATION_PERCENT,

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
    val sizeRatio: Double = DEFAULT_SIZE_RATIO,

    /**
     * Configures the "Size Ratio Trigger" for compaction.
     *
     * Size-Ratio-based compaction will only occur if at least [minMergeTiers] are affected.
     */
    val minMergeTiers: Int = DEFAULT_MIN_MERGE_TIERS,

    /**
     * The strategy to apply when deciding when to split an outbound data stream into multiple LSM files.
     *
     * Defaults to [FileSeparationStrategy.SizeBased].
     */
    override val fileSeparationStrategy: FileSeparationStrategy = DEFAULT_FILE_SEPARATION_STRATEGY,
) : CompactionStrategy {

    companion object {

        private val DEFAULT_NUMBER_OF_TIERS: Int = 8

        private val DEFAULT_MAX_SPACE_AMPLIFICATION_PERCENT: Double = 2.0

        private val DEFAULT_SIZE_RATIO: Double = 1.0

        private val DEFAULT_MIN_MERGE_TIERS: Int = 2

        private val DEFAULT_FILE_SEPARATION_STRATEGY: FileSeparationStrategy = FileSeparationStrategy.SizeBased()

        @JvmStatic
        fun builder(): Builder {
            return Builder()
        }

    }

    /**
     * A Builder for a [org.lsm4k.api.compaction.TieredCompactionStrategy].
     */
    class Builder {

        private var numberOfTiers: Int = DEFAULT_NUMBER_OF_TIERS

        private var maxSpaceAmplificationPercent: Double = DEFAULT_MAX_SPACE_AMPLIFICATION_PERCENT

        private var sizeRatio: Double = DEFAULT_SIZE_RATIO

        private var minMergeTiers: Int = DEFAULT_MIN_MERGE_TIERS

        private var fileSeparationStrategy: FileSeparationStrategy = DEFAULT_FILE_SEPARATION_STRATEGY

        /**
         * @see [org.lsm4k.api.compaction.TieredCompactionStrategy.numberOfTiers]
         */
        fun withNumberOfTiers(numberOfTiers: Int): Builder {
            this.numberOfTiers = numberOfTiers
            return this
        }

        /**
         * @see [org.lsm4k.api.compaction.TieredCompactionStrategy.maxSpaceAmplificationPercent]
         */
        fun withMaxSpaceAmplificationPercent(maxSpaceAmplificationPercent: Double): Builder {
            this.maxSpaceAmplificationPercent = maxSpaceAmplificationPercent
            return this
        }

        /**
         * @see [org.lsm4k.api.compaction.TieredCompactionStrategy.sizeRatio]
         */
        fun withSizeRatio(sizeRatio: Double): Builder {
            this.sizeRatio = sizeRatio
            return this
        }

        /**
         * @see [org.lsm4k.api.compaction.TieredCompactionStrategy.minMergeTiers]
         */
        fun withMinMergeTiers(minMergeTiers: Int): Builder {
            this.minMergeTiers = minMergeTiers
            return this
        }

        /**
         * @see [org.lsm4k.api.compaction.TieredCompactionStrategy.fileSeparationStrategy]
         */
        fun withFileSeparationStrategy(fileSeparationStrategy: FileSeparationStrategy): Builder {
            this.fileSeparationStrategy = fileSeparationStrategy
            return this
        }

        /**
         * Finalizes the [org.lsm4k.api.compaction.TieredCompactionStrategy] and returns it.
         *
         * @return The finalized strategy.
         */
        fun build(): TieredCompactionStrategy {
            return TieredCompactionStrategy(
                numberOfTiers = this.numberOfTiers,
                maxSpaceAmplificationPercent = this.maxSpaceAmplificationPercent,
                sizeRatio = this.sizeRatio,
                minMergeTiers = this.minMergeTiers,
                fileSeparationStrategy = this.fileSeparationStrategy,
            )
        }

    }

    override fun toString(): String {
        return "TieredCompactionStrategy[tiers=${this.numberOfTiers}, sizeAmp=${String.format("%.2f", this.maxSpaceAmplificationPercent)}, sizeRatio=${String.format("%.2f", this.sizeRatio)}]"
    }

}