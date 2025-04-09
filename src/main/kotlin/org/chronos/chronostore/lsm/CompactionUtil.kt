package org.chronos.chronostore.lsm

import com.google.common.collect.Iterators
import org.chronos.chronostore.io.format.writer.ChronoStoreFileWriter
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTSN
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.iterator.IteratorExtensions.checkOrdered
import org.chronos.chronostore.util.iterator.IteratorExtensions.dropHistoryOlderThan
import org.chronos.chronostore.util.iterator.IteratorExtensions.filter
import org.chronos.chronostore.util.iterator.IteratorExtensions.latestVersionOnly
import org.chronos.chronostore.util.iterator.IteratorExtensions.orderedDistinct

object CompactionUtil {

    /**
     * Compacts the given [cursors] into the given [writer].
     *
     * The current position of the cursors will be ignored. This method reads all of their values from first to last
     * and merges the results.
     *
     * @param cursors The cursors to use as input for the compaction
     * @param writer The writer which receives the output data.
     * @param keepTombstones Decides what to do if the final merged operation for any key is a deletion (a.k.a. a tombstone).
     *                       Use `true` if the deletion should be kept and written into the [writer], or `false` if the deletion
     *                       should be dropped. Typically, when compacting intermediate levels/tiers, you'll want to keep the
     *                       deletions to allow them to propagate to the highest level/tier. When compacting into the highest
     *                       level/tier, you'll typically want to drop the tombstones.
     * @param resultingCommandCountEstimate An estimate on how many commands will be in the final resulting file. Will be used for sizing the bloom filter.
     * @param maxNumberOfMergesInInputFiles The highest number of received merges across all input files. The output file will have a "numberOfMerges"
     *                 equal to [maxMerge] + 1. For statistical purposes only.
     * @param maxCompletelyWrittenTSN The highest [TSN] which has been fully written in the input files. May be `null` if no transaction has been fully written yet.
     * @param smallestReadTSN Among all currently open transactions, this is the smallest [TSN] used for reading. In other words, this is the oldest
     *                        TSN which still needs to be accessible after the merge. All older historic data will be discarded during the compaction.
     *                        If this value is `null`, all historic entries will be discarded during the transaction.
     */
    fun compact(
        cursors: List<Cursor<KeyAndTSN, Command>>,
        writer: ChronoStoreFileWriter,
        keepTombstones: Boolean,
        resultingCommandCountEstimate: Long,
        maxNumberOfMergesInInputFiles: Long,
        maxCompletelyWrittenTSN: TSN?,
        smallestReadTSN: TSN?,
    ) {
        val iterators = cursors.mapNotNull {
            if (it.first()) {
                it.ascendingValueSequenceFromHere().iterator()
            } else {
                null
            }
        }
        val commands = Iterators.mergeSorted(iterators, Comparator.naturalOrder())
        // ensure ordering and remove duplicates (which is cheap and lazy for ordered iterators)
        val basicIterator = commands.checkOrdered(strict = false).orderedDistinct()

        // we have to drop old versions of keys
        val latestVersionIterator = if (smallestReadTSN == null) {
            // there are no open transactions, we can drop ALL historic entries.
            basicIterator.latestVersionOnly()
        } else {
            // drop all historic entries which are smaller than the read TSN.
            basicIterator.dropHistoryOlderThan(smallestReadTSN)
        }

        val finalIterator = if (keepTombstones) {
            latestVersionIterator
        } else {
            latestVersionIterator.filter(Command::isInsertOrUpdate)
        }

        writer.write(
            numberOfMerges = maxNumberOfMergesInInputFiles + 1,
            orderedCommands = finalIterator,
            commandCountEstimate = resultingCommandCountEstimate,
            maxCompletelyWrittenTSN = maxCompletelyWrittenTSN,
        )
    }

}