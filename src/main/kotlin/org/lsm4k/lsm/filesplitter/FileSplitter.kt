package org.lsm4k.lsm.filesplitter

import org.lsm4k.api.compaction.FileSeparationStrategy
import org.lsm4k.lsm.LSMTree
import org.lsm4k.util.bytes.Bytes

/**
 * A [FileSplitter] is responsible for splitting LSM files into smaller files.
 *
 * Each splitter executes the [FileSeparationStrategy] for which it was created.
 *
 * To create an instance of this class, please use [Companion.createSplitterForTree].
 */
sealed interface FileSplitter {

    companion object {

        /**
         * Creates a new [FileSplitter] for the given [lsmTree].
         *
         * @param lsmTree The [LSMTree] to operate on. Needed to fetch metadata.
         *
         * @return The new file splitter.
         */
        fun createSplitterForTree(lsmTree: LSMTree): FileSplitter {
            return when (val separationStrategy = lsmTree.getStoreMetadata().compactionStrategy.fileSeparationStrategy) {
                is FileSeparationStrategy.SingleFile -> SingleFileSplitter
                is FileSeparationStrategy.SizeBased -> SizeBasedFileSplitter(separationStrategy.individualFileSize.bytes)
            }
        }

    }

    /**
     * Decides if the file should be split here.
     *
     * @param fileSizeInBytes The current file size in bytes (might be an estimate)
     * @param firstKeyInFile The first key in the file which is currently being written. `null` if no key was written yet.
     * @param lastKeyInFile The last key which has been written so far to the current file. `null` if no key was written yet.
     *
     * @return `true` if the file should be "split" here (i.e. we conclude the current file and start the next one). `false` if the file should receive more data.
     */
    fun splitHere(
        fileSizeInBytes: Long,
        firstKeyInFile: Bytes?,
        lastKeyInFile: Bytes?,
    ): Boolean

}