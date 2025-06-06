package org.lsm4k.io.format.writer

import org.lsm4k.io.format.LSMFileSettings
import org.lsm4k.io.vfs.VirtualReadWriteFile
import org.lsm4k.lsm.filesplitter.FileSplitter
import org.lsm4k.model.command.Command
import org.lsm4k.util.TSN
import org.lsm4k.util.bytes.Bytes
import org.lsm4k.util.statistics.StatisticsReporter

/**
 * A [LSMStoreFileWriter] which writes one **or more** files based on the given [fileSplitter].
 */
class SplitLSMStoreFileWriter(
    /** The file splitter to use. */
    private val fileSplitter: FileSplitter,
    /** The settings to use for new files created by this writer. */
    private val newFileSettings: LSMFileSettings,

    private val statisticsReporter: StatisticsReporter,
    /** Function to open the next free file in the LSM tree. */
    private val openNextFile: () -> VirtualReadWriteFile,
) : LSMStoreFileWriter {

    private var closed = false

    private val filesWrittenSoFar = mutableSetOf<VirtualReadWriteFile>()

    override fun write(numberOfMerges: Long, orderedCommands: Iterator<Command>, commandCountEstimate: Long, maxCompletelyWrittenTSN: TSN?) {
        this.assertNotClosed()
        try {
            // the basic idea here is as follows:
            //
            // - We use the Iterator<Command>, and apply a "splitting" iterator on it. This splitting iterator
            //   keeps drawing elements from the ordered commands iterator until either the fileSplitter tells us
            //   to stop OR we run out of commands in the original iterator.
            //
            // - So whenever our basic file writer says that it's "done", this can mean one of two things:
            //
            //   - We ran out of commands. In this case we're done overall.
            //
            //   - There are more commands, but the current file is full.
            //     In that case we simply open the next file, create a new splitting iterator on the original one,
            //     and continue the same algorithm until we've exhausted the original iterator.

            // write all commands, only stop when there are no more commands
            while (orderedCommands.hasNext()) {
                this.assertNotClosed()

                // get the next file, and write commands into it until the splitter tells us to stop,
                // or we run out of commands (whatever happens first)
                this.filesWrittenSoFar += writeNextFile(
                    orderedCommands = orderedCommands,
                    numberOfMerges = numberOfMerges,
                    commandCountEstimate = commandCountEstimate,
                    maxCompletelyWrittenTSN = maxCompletelyWrittenTSN,
                )
            }
        } catch (e: Exception) {
            this.close()
            throw e
        }
    }

    private fun writeNextFile(
        orderedCommands: Iterator<Command>,
        numberOfMerges: Long,
        commandCountEstimate: Long,
        maxCompletelyWrittenTSN: TSN?,
    ): VirtualReadWriteFile {
        // get or create the next free LSM file
        val nextFile = this.openNextFile()
        nextFile.deleteOverWriterFileIfExists()
        nextFile.createOverWriter().use { overWriter ->
            // use the standard LSM file writer...
            StandardLSMStoreFileWriter(overWriter.outputStream, this.newFileSettings, this.statisticsReporter).use { writer ->
                // ... but use only a "prefix" of the iterator (it operates like a "takeWhile" on kotlin sequences)
                val commandsInFileIterator = FileSplittingIterator(orderedCommands, this.fileSplitter)

                // write the commands that belong to this file into this file
                writer.write(
                    numberOfMerges = numberOfMerges,
                    orderedCommands = commandsInFileIterator,
                    // this is not ideal: we don't know upfront how many files we will create, so
                    // we have to take the "defensive" option and assume that we create a single
                    // file with ALL data in it. This may lead to oversized bloom filters
                    // TODO [PERFORMANCE]: Find a better estimate here, this may lead to oversized bloom filters
                    commandCountEstimate = commandCountEstimate,
                    maxCompletelyWrittenTSN = maxCompletelyWrittenTSN,
                )
            }

            overWriter.commit()
        }
        return nextFile
    }

    val writtenFiles: Set<VirtualReadWriteFile>
        get() {
            return this.filesWrittenSoFar.toSet()
        }

    override fun close() {
        if (closed) {
            return
        }
        closed = true
    }

    private fun assertNotClosed() {
        check(!this.closed) { "This ${SplitLSMStoreFileWriter::class.simpleName} has already been closed!" }
    }

    private class FileSplittingIterator(
        val wrapped: Iterator<Command>,
        val splitter: FileSplitter,
    ) : Iterator<Command> {

        private var bytesWritten: Long = 0L
        private var firstReturnedKey: Bytes? = null
        private var lastReturnedKey: Bytes? = null

        override fun hasNext(): Boolean {
            if (!this.wrapped.hasNext()) {
                // we're out of entries
                return false
            }
            val shouldSplitHere = this.splitter.splitHere(
                fileSizeInBytes = this.bytesWritten,
                firstKeyInFile = this.firstReturnedKey,
                lastKeyInFile = this.lastReturnedKey,
            )
            // if we should split here, we DON'T have a "next" entry.
            // if we should NOT split here, we have a "next" entry.
            return !shouldSplitHere
        }

        override fun next(): Command {
            if (!this.hasNext()) {
                throw NoSuchElementException("SplitIterator is exhausted!")
            }
            val next = this.wrapped.next()

            // remember the first key returned by this method
            if (this.firstReturnedKey == null) {
                this.firstReturnedKey = next.key
            }

            // remember the last key by this method
            this.lastReturnedKey = next.key

            // aggregate the bytes returned by this method
            this.bytesWritten += next.byteSize

            return next
        }

    }
}