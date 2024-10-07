package org.chronos.chronostore.lsm

import com.google.common.collect.Iterators
import org.chronos.chronostore.io.format.ChronoStoreFileWriter
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTSN
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.iterator.IteratorExtensions.checkOrdered
import org.chronos.chronostore.util.iterator.IteratorExtensions.filter
import org.chronos.chronostore.util.iterator.IteratorExtensions.latestVersionOnly
import org.chronos.chronostore.util.iterator.IteratorExtensions.orderedDistinct

object CompactionUtil {

    fun compact(
        cursors: List<Cursor<KeyAndTSN, Command>>,
        writer: ChronoStoreFileWriter,
        maxMerge: Long,
        totalEntries: Long,
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

        // we have to drop old versions...
        val finalIterator = basicIterator.latestVersionOnly()
            // ...and if the latest version happens to be a DELETE, we ignore the key.
            .filter { it.opCode != Command.OpCode.DEL }
        writer.writeFile(
            numberOfMerges = maxMerge + 1,
            orderedCommands = finalIterator,
            commandCountEstimate = totalEntries,
        )
    }

}