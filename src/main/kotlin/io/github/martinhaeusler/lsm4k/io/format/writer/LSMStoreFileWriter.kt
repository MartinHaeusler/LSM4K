package io.github.martinhaeusler.lsm4k.io.format.writer

import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.util.TSN

interface LSMStoreFileWriter : AutoCloseable {

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