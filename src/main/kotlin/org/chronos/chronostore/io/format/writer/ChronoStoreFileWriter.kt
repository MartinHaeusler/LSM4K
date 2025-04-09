package org.chronos.chronostore.io.format.writer

import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.TSN

interface ChronoStoreFileWriter : AutoCloseable {

    /**
     * Writes the given [orderedCommands] (as well as any required additional information) to LSM file(s).
     *
     * @param numberOfMerges The number of merges to record in the file metadata.
     * @param orderedCommands The sequence of commands to write. **MUST** be ordered!
     * @param commandCountEstimate An estimate of the total number of entries in [orderedCommands]. Will be used for bloom filter sizing.
     * @param maxCompletelyWrittenTSN The highest [TSN] which is guaranteed to be completely contained in this file (or previous already written files).
     *                                    May be `null` if no transaction has been fully completed.
     */
    fun write(
        numberOfMerges: Long,
        orderedCommands: Iterator<Command>,
        commandCountEstimate: Long,
        maxCompletelyWrittenTSN: TSN?,
    )

}