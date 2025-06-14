package io.github.martinhaeusler.lsm4k.api.compaction

import io.github.martinhaeusler.lsm4k.impl.annotations.PersistentClass
import io.github.martinhaeusler.lsm4k.lsm.compaction.model.CompactableFile
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.MiB

/**
 * Leveled compaction divides the SST files into levels.
 *
 * # Trade-Offs and Use Cases
 *
 * This strategy aims for a lower degree of read amplification in exchange for higher
 * write amplification. This means that queries will generally be faster, but it comes
 * at the cost of a higher rate of data rewriting on disk and potentially lower ingestion
 * rates. This strategy is very suitable for read-mostly scenarios.
 *
 * # Key Concepts
 *
 * - **Data that changes frequently lives at low levels, data that changes rarely lives at high levels.**
 *
 *    This is the foundational assumption of leveled compaction. Keys which get overwritten often
 *    stay in lower levels where SST files are smaller and their compaction is therefore cheaper
 *    to perform (in terms of performance and IO operations).
 *
 * - **New files produced by flush tasks are always inserted at level 0.**
 *
 *    This has no impact on the other levels or files, they all stay the same.
 *
 * - **Level 0 contains multiple sorted runs.**
 *
 *    Level 0 is a special level because it allows multiple flush results (sorted runs)
 *    to live within this level.
 *
 * - **Higher Levels contain one sorted run.**
 *
 *    Levels higher than 0 only contain one sorted run, i.e. each key only occurs once
 *    within **all** of the SST files in that level. Furthermore, each SST file defines
 *    a key range, and the key ranges within a single level **must not overlap**.
 *
 * - **Compaction occurs from one level to the next.**
 *
 *    When one or more SST files within a level get selected for compaction, their
 *    total key range is checked and compared against the next level only. This gives
 *    the SST files in the next level which are going to be compacted as well. The new
 *    SST files then again are guaranteed to have non-overlapping key ranges inside the
 *    higher level again (otherwise we would have selected more SST files from that level).
 *
 * - **There is a size ratio between the levels.**
 *
 *    A fixed desired ratio between each level and the level below it is given (e.g.
 *    each level must be at most 10% of the size of the next-higher level). If this
 *    ratio is exceeded by a level, this level is selected for compaction. This process
 *    cascades until all levels fulfill the size ratio requirement. This means in the
 *    worst case that the highest level needs to grow.
 *
 * # Configuration
 *
 * - **Level 0 File Count**
 *
 *     In contrast to all higher levels (where all files in total have disjointed
 *     key sets from one another), at Level 0 each file contains its own keyset
 *     and arbitrary overlaps are possible. Furthermore, the level size trigger
 *     does not apply to it. In order to compact files from Level 0 to higher
 *     levels, we track the number of files in Level 0 and if it is above
 *     [level0FileNumberCompactionTrigger], a compaction of Level 0 is triggered.
 *
 * - **Level Size**
 *
 *    The primary trigger for leveled compaction (on levels 1 or higher) is when a
 *    level becomes too big according to the [levelSizeMultiplier]. This triggers
 *    a compaction of said level. When multiple levels trigger this condition at
 *    the same time, the level with the highest relative difference to its target
 *    size will be compacted first.
 *
 *    The [baseLevelMinSize] gives the minimum size for the base level (L1). From
 *    this value, each next-higher level will get its target size by multiplying
 *    with the [levelSizeMultiplier]. If L1 is smaller than this value, no
 *    compactions into higher levels will be triggered.
 *
 * - **Maximum Number of Levels**
 *
 *     In leveled compaction, a pre-determined [maximum number of levels][maxLevels]
 *     is given. This number needs to be greater than zero, and always excludes
 *     Level 0. For example, a value of 2 means that there will be levels 0, 1
 *     and 2.
 */
@PersistentClass(format = PersistentClass.Format.JSON, details = "Used in Manifest.")
class LeveledCompactionStrategy(
    /**
     * Controls the target size of levels relative to each other.
     *
     * The target size of the next-higher level is [levelSizeMultiplier] times the
     * target size of the current level.
     */
    val levelSizeMultiplier: Double = DEFAULT_LEVEL_SIZE_MULTIPLIER,

    /**
     * The number of files allowed in level 0.
     *
     * Any further insertion will trigger a compaction.
     */
    val level0FileNumberCompactionTrigger: Int = DEFAULT_LEVEL_0_FILE_NUMBER_COMPACTION_TRIGGER,

    /**
     * The maximum number of levels to use in the LSM tree.
     */
    val maxLevels: Int = DEFAULT_MAX_LEVELS,

    /**
     * The minimum size of the highest level of the LSM tree which needs to be reached before
     * the relative level size compaction triggers are used.
     */
    val baseLevelMinSize: BinarySize = DEFAULT_BASE_LEVEL_MIN_SIZE,

    /**
     * The strategy to apply when selecting which files from within a level should be compacted first.
     */
    val fileSelectionStrategy: FileSelectionStrategy = DEFAULT_FILE_SELECTION_STRATEGY,

    /**
     * The strategy to apply when deciding when to split an outbound data stream into multiple LSM files.
     *
     * Defaults to [FileSeparationStrategy.SizeBased].
     */
    override val fileSeparationStrategy: FileSeparationStrategy = DEFAULT_FILE_SEPARATION_STRATEGY,
) : CompactionStrategy {

    companion object {

        private val DEFAULT_LEVEL_SIZE_MULTIPLIER: Double = 10.0

        private val DEFAULT_LEVEL_0_FILE_NUMBER_COMPACTION_TRIGGER: Int = 5

        private val DEFAULT_MAX_LEVELS: Int = 8

        private val DEFAULT_BASE_LEVEL_MIN_SIZE: BinarySize = 200.MiB

        private val DEFAULT_FILE_SELECTION_STRATEGY: FileSelectionStrategy = FileSelectionStrategy.DEFAULT

        private val DEFAULT_FILE_SEPARATION_STRATEGY: FileSeparationStrategy = FileSeparationStrategy.SizeBased()

        @JvmStatic
        fun builder(): Builder {
            return Builder()
        }

    }

    /**
     * A Builder for a [org.lsm4k.api.compaction.LeveledCompactionStrategy].
     */
    class Builder {

        private var levelSizeMultiplier: Double = DEFAULT_LEVEL_SIZE_MULTIPLIER

        private var level0FileNumberCompactionTrigger: Int = DEFAULT_LEVEL_0_FILE_NUMBER_COMPACTION_TRIGGER

        private var maxLevels: Int = DEFAULT_MAX_LEVELS

        private var baseLevelMinSize: BinarySize = DEFAULT_BASE_LEVEL_MIN_SIZE

        private var fileSelectionStrategy: FileSelectionStrategy = DEFAULT_FILE_SELECTION_STRATEGY

        private var fileSeparationStrategy: FileSeparationStrategy = DEFAULT_FILE_SEPARATION_STRATEGY

        /**
         * @see [org.lsm4k.api.compaction.LeveledCompactionStrategy.levelSizeMultiplier]
         */
        fun withLevelSizeMultiplier(levelSizeMultiplier: Double): Builder {
            this.levelSizeMultiplier = levelSizeMultiplier
            return this
        }

        /**
         * @see [org.lsm4k.api.compaction.LeveledCompactionStrategy.level0FileNumberCompactionTrigger]
         */
        fun withLevel0FileNumberCompactionTrigger(level0FileNumberCompactionTrigger: Int): Builder {
            this.level0FileNumberCompactionTrigger = level0FileNumberCompactionTrigger
            return this
        }

        /**
         * @see [org.lsm4k.api.compaction.LeveledCompactionStrategy.maxLevels]
         */
        fun withMaxLevels(maxLevels: Int): Builder {
            this.maxLevels = maxLevels
            return this
        }

        /**
         * @see [org.lsm4k.api.compaction.LeveledCompactionStrategy.baseLevelMinSize]
         */
        fun withBaseLevelMinSize(baseLevelMinSize: BinarySize): Builder {
            this.baseLevelMinSize = baseLevelMinSize
            return this
        }

        /**
         * @see [org.lsm4k.api.compaction.LeveledCompactionStrategy.fileSelectionStrategy]
         */
        fun withFileSelectionStrategy(fileSelectionStrategy: FileSelectionStrategy): Builder {
            this.fileSelectionStrategy = fileSelectionStrategy
            return this
        }

        /**
         * @see [org.lsm4k.api.compaction.LeveledCompactionStrategy.fileSeparationStrategy]
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
        fun build(): LeveledCompactionStrategy {
            return LeveledCompactionStrategy(
                levelSizeMultiplier = this.levelSizeMultiplier,
                level0FileNumberCompactionTrigger = this.level0FileNumberCompactionTrigger,
                maxLevels = this.maxLevels,
                baseLevelMinSize = this.baseLevelMinSize,
                fileSelectionStrategy = this.fileSelectionStrategy,
                fileSeparationStrategy = this.fileSeparationStrategy,
            )
        }

    }

    enum class FileSelectionStrategy {

        /**
         * Picks the file with the highest ratio of deletions and overwrites (i.e. more than one
         * timestamp entry per key).
         *
         * This strategy avoids the worst case for LSM trees where a large portion of a database
         * is deleted. This strategy allows these deletions to "travel faster" by compacting them
         * earlier.
         *
         * This is the default strategy.
         */
        BY_MOST_DELETIONS {

            override val comparator: Comparator<CompactableFile> = compareBy(
                // first compare by head-history-ratio.
                // Files with a lot of history have a lower value here. Since we're sorting
                // ascending, these files will be higher in priority.
                { it.metadata.headHistoryRatio },
                // as a tie-breaker, compare by the oldest data.
                { it.metadata.minTSN },
                // as a tie-breaker, compare by the index.
                { it.index }
            )

        },

        /**
         * Picks the file that covers the oldest updates in the level.
         *
         * In other words, compares files by their smallest contained [TSN]. Ties will be broken
         * by taking the oldest file.
         *
         * These files usually contain the highest density of keys (small key range, many keys).
         * Compacting these files first reduces write amplification, provided that the overall
         * keys in the store are approximately uniformly distributed.
         */
        OLDEST_SMALLEST_SEQUENCE_FIRST {

            override val comparator: Comparator<CompactableFile> = compareBy(
                { it.metadata.minTSN },
                // as a tie-breaker, compare by the index.
                { it.index }
            )

        },

        /**
         * Picks the file whose latest update is the oldest.
         *
         * In other words, compare files by their largest contained [TSN]. Ties will be broken
         * by taking the oldest file.
         *
         * These files contain the key ranges with the oldest updates (i.e. "cold" key ranges).
         * Compacting the coldest files into higher levels means that "hot" key ranges which
         * are updated frequently will reside in the lower levels of the tree, therefore
         * frequent updates can eliminate each other earlier.
         *
         * This strategy is best suited when there is a small range of keys which is overwritten
         * very often, while the rest of the keys is updated rarely.
         */
        OLDEST_LARGEST_SEQUENCE_FIRST {
            override val comparator: Comparator<CompactableFile> =
                // first, prioritize the newest files (highest max TSN)
                compareByDescending<CompactableFile> { it.metadata.maxTSN }
                    // break ties by comparing the index.
                    .thenComparingInt { it.index }
        },

        ;

        companion object {

            /** Defaults to [BY_MOST_DELETIONS]. */
            val DEFAULT = BY_MOST_DELETIONS

        }

        /** Creates a file comparator that corresponds with this strategy. */
        abstract val comparator: Comparator<CompactableFile>

    }

    override fun toString(): String {
        val sizeMult = String.format("%.2f", this.levelSizeMultiplier)
        return "LeveledCompactionStrategy[levels=${this.maxLevels}, sizeMult=${sizeMult}, baseLevelSize=${this.baseLevelMinSize} selection=${this.fileSelectionStrategy}]"
    }

}