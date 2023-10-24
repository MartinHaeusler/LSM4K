package org.chronos.chronostore.wal

import com.google.common.io.CountingInputStream
import mu.KotlinLogging
import org.chronos.chronostore.api.exceptions.TruncatedInputException
import org.chronos.chronostore.api.exceptions.WriteAheadLogEntryCorruptedException
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.io.structure.ChronoStoreStructure.WRITE_AHEAD_LOG_FILE_PREFIX
import org.chronos.chronostore.io.structure.ChronoStoreStructure.WRITE_AHEAD_LOG_FILE_SUFFIX
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.StreamExtensions.hasNext
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.iterator.IteratorExtensions.peekOrNull
import org.chronos.chronostore.util.iterator.IteratorExtensions.toPeekingIterator
import org.chronos.chronostore.util.unit.MiB
import java.io.InputStream
import java.io.OutputStream
import java.io.PushbackInputStream
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write


/**
 * The [WriteAheadLog] class manages the Write-Ahead-Log (WAL) of the store.
 *
 * The WAL is stored in a single [directory] and can consist of multiple files.
 * Each file is written in an append-only fashion. For a description of the file
 * format, please see [WriteAheadLogFormat]. Each file has a maximum size given
 * by [maxWalFileSizeBytes] (please note that this is not a hard limit; no further
 * data will be stored in a WAL file **after** this size has been reached, but
 * one transaction will always be stored in exactly one file).
 *
 * When a WAL file becomes full, the next WAL file in sequence is opened; this
 * is sometimes referred to as "log segmentation". The main purpose of this
 * behavior is to easily delete old "chunks" of the WAL which are not needed
 * anymore because all of their content has already been (persistently) written
 * to the actual stores.
 *
 * In order to determine which WAL files can be discarded, we consider the
 * largest transaction timestamp which has been (persistently) written by all
 * stores. This is called the "low watermark". If the transaction with the
 * highest timestamp in a WAL file is less than or equal to the low watermark,
 * we can safely discard the WAL file because all of its data has already been
 * transferred to and been persisted by the actual stores.
 *
 * @param directory The directory where the WAL files are stored.
 * @param compressionAlgorithm The compression algorithm to use when creating new WAL files. Existing WAL files may use other algorithms.
 * @param maxWalFileSizeBytes The maximum size for a single WAL file (soft limit).
 *
 */
class WriteAheadLog(
    private val directory: VirtualDirectory,
    private val compressionAlgorithm: CompressionAlgorithm = CompressionAlgorithm.SNAPPY,
    private val maxWalFileSizeBytes: Long = 128.MiB.bytes,
    private val minNumberOfFiles: Int = 1,
) {

    companion object {

        private val log = KotlinLogging.logger {}

        private val FILE_NAME_REGEX = """$WRITE_AHEAD_LOG_FILE_PREFIX(\d+)$WRITE_AHEAD_LOG_FILE_SUFFIX""".toRegex()

    }

    private val lock = ReentrantReadWriteLock(true)

    private val walFiles = mutableListOf<WALFile>()

    init {
        require(maxWalFileSizeBytes > 0) { "Argument 'maxWalFileSizeBytes' (${maxWalFileSizeBytes}) must be positive!" }
        require(minNumberOfFiles > 0) { "Argument 'minNumberOfFiles' (${minNumberOfFiles}) must be positive!" }
        if (!this.directory.exists()) {
            this.directory.mkdirs()
        }
        this.directory.listFiles().asSequence()
            .mapNotNull(::createWALFileOrNull)
            .sortedBy { it.minTimestamp }
            .forEach(walFiles::add)
    }

    private fun createWALFileOrNull(file: VirtualFile): WALFile? {
        if (file !is VirtualReadWriteFile) {
            return null
        }
        val match = FILE_NAME_REGEX.matchEntire(file.name)
            ?: return null
        val sequenceNumber = match.groups[1]?.value?.toLong()
            ?: return null
        return WALFile(file, sequenceNumber)
    }

    /**
     * Adds a commit to the Write Ahead Log file.
     *
     * This operation requires the write lock on the WAL file; it may be blocked until
     * concurrent reads or writes have been completed.
     *
     * @param walTransaction The transaction to add to the file.
     */
    fun addCommittedTransaction(walTransaction: WriteAheadLogTransaction) {
        this.lock.write {
            val targetFile = getOrCreateTargetWALFileForTransactionTimestamp(walTransaction.commitTimestamp)
            targetFile.append { out ->
                WriteAheadLogFormat.writeTransaction(walTransaction, this.compressionAlgorithm, out)
            }
        }
    }

    private fun getOrCreateTargetWALFileForTransactionTimestamp(newTransactionTimestamp: Timestamp): WALFile {
        val currentWALFile = this.walFiles.lastOrNull()
        if (currentWALFile != null && !currentWALFile.isFull(this.maxWalFileSizeBytes)) {
            // we still have room in our current file.
            check(currentWALFile.minTimestamp <= newTransactionTimestamp) {
                "Cannot write transaction with commit timestamp ${newTransactionTimestamp} into WAL file with min timestamp ${currentWALFile.minTimestamp}!"
            }
            return currentWALFile
        }
        // current WAL file either doesn't exist or is full.
        val newFile = this.directory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}${newTransactionTimestamp}${WRITE_AHEAD_LOG_FILE_SUFFIX}")
        newFile.create()
        val newWALFile = WALFile(newFile, newTransactionTimestamp)
        this.walFiles.add(newWALFile)
        return newWALFile
    }

    /**
     * Sequentially reads the Write Ahead Log, permitting a given [consumer] to read the contained transactions one after another.
     *
     * If the file contains invalid entries (e.g. incomplete transactions due to a system shutdown during WAL writes), these
     * entries will **not** be reported to the [consumer]. The consumer will receive the transactions in ascending timestamp order.
     *
     * @param consumer The consumer function which will receive the transactions in the write-ahead log for further processing.
     *                 The transactions will be provided to the consumer in a lazy fashion, in ascending timestamp order.
     */
    fun readWalStreaming(consumer: (WriteAheadLogTransaction) -> Unit) {
        this.lock.read {
            var previousTxCommitTimestamp = -1L
            for (walFile in this.walFiles) {
                walFile.withInputStream { input ->
                    PushbackInputStream(input).use { pbIn ->
                        while (pbIn.hasNext()) {
                            val tx = WriteAheadLogFormat.readTransaction(pbIn)
                                ?: break
                            if (tx.commitTimestamp <= previousTxCommitTimestamp) {
                                throw WriteAheadLogEntryCorruptedException("Found non-ascending commit timestamp in the WAL! The WAL file is corrupted!")
                            }
                            consumer(tx)
                            previousTxCommitTimestamp = tx.commitTimestamp
                        }
                    }
                }
            }
        }
    }

    /**
     * Checks if the WAL has more than the configured minimum number of files.
     *
     * @return `true` if there are too many files and the WAL should be [shortened][shorten], otherwise `false`.
     */
    fun needsToBeShortened(): Boolean {
        return this.lock.read { this.walFiles.size > this.minNumberOfFiles }
    }

    /**
     * Attempts to reduce the size of the write-ahead-log file by discarding fully persisted transactions.
     *
     * By "fully persisted transactions" we mean transactions for which no parts reside
     * in the in-memory segments of any involved LSM trees. In other words, a transaction
     * is "fully persisted" if all modifications have been written into persistent chronostore
     * files.
     *
     * This method requires the write lock on the Write-Ahead-Log and may require a substantial amount of time
     * to complete; it should be used with care and only during periods of low database utilization, because
     * no commits will be permitted until this method has been completed.
     *
     * Please note that calling this method has no effect if [needsToBeShortened] returns `false`.
     *
     * @param lowWatermarkTimestamp The maximum timestamp for which it is guaranteed that *all* stores have
     *                              persistently stored the results of *all* transactions with commit timestamps
     *                              less than or equal to the watermark.
     */
    fun shorten(lowWatermarkTimestamp: Timestamp) {
        this.lock.write {
            if (!this.needsToBeShortened()) {
                return
            }
            val iterator = this.walFiles.iterator().toPeekingIterator()
            val filesToDelete = mutableSetOf<WALFile>()
            while (iterator.hasNext()) {
                val currentWALFile = iterator.next()
                val nextWALFile = iterator.peekOrNull()
                    ?: break // the currentWALFile is actually the LAST file in our WAL, don't drop it.

                if (currentWALFile.minTimestamp < lowWatermarkTimestamp && nextWALFile.minTimestamp < lowWatermarkTimestamp) {
                    // the current file is below the watermark, because the HIGHEST timestamp in this file is strictly
                    // smaller than the min timestamp of the next file. Since that is below the watermark, we DEFINITELY
                    // are below the watermark.
                    filesToDelete += currentWALFile
                }
            }
            this.walFiles.removeAll(filesToDelete)
            filesToDelete.forEach(WALFile::delete)
        }
    }

    /**
     * Performs the startup recovery on the Write Ahead Log file.
     *
     * In particular, this method will remove any invalid or incomplete transactions from the WAL.
     */
    fun performStartupRecoveryCleanup(getHighWatermarkTimestamp: () -> Timestamp) {
        this.lock.write {
            for ((index, walFile) in this.walFiles.withIndex()) {
                // we tolerate incomplete trailing writes on the last file.
                val isLastFile = index == this.walFiles.lastIndex

                walFile.withInputStream { input ->
                    PushbackInputStream(input).use { pbIn ->
                        CountingInputStream(pbIn).use { cIn ->
                            var startOfBlock: Long
                            var lastSuccessfulTransactionTimestamp = -1L
                            while (pbIn.hasNext()) {
                                startOfBlock = cIn.count
                                try {
                                    val tx = WriteAheadLogFormat.readTransaction(cIn)
                                        ?: break // we're done reading this file
                                    lastSuccessfulTransactionTimestamp = tx.commitTimestamp
                                } catch (e: TruncatedInputException) {
                                    if (isLastFile) {
                                        // the last file in our WAL may become truncated if the last
                                        // commit was interrupted by process kill or power outage. We
                                        // can tolerate that, as long as no store has ever seen a
                                        // higher timestamp than the previous entry.
                                        if (getHighWatermarkTimestamp() > lastSuccessfulTransactionTimestamp) {
                                            throw WriteAheadLogEntryCorruptedException("WAL file '${walFile.file.path}' is has been truncated and cannot be read!")
                                        }
                                        // truncate the file
                                        walFile.file.truncateAfter(startOfBlock)
                                    } else {
                                        // for all other WAL files (which are not the last one) we cannot tolerate missing data.
                                        throw WriteAheadLogEntryCorruptedException("WAL file '${walFile.file.path}' is has been truncated and cannot be read!")
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private class WALFile(
        val file: VirtualReadWriteFile,
        val minTimestamp: Timestamp,
    ) {

        init {
            require(minTimestamp >= 0) { "Argument 'minTimestamp' (${minTimestamp}) must not be negative!" }
        }

        fun isFull(maxWalFileSizeBytes: Long): Boolean {
            return this.length >= maxWalFileSizeBytes
        }

        val length: Long
            get() = this.file.length

        fun <T> append(action: (OutputStream) -> T): T {
            return this.file.append(action)
        }

        fun <T> withInputStream(action: (InputStream) -> T): T {
            return this.file.withInputStream(action)
        }

        fun delete() {
            return this.file.delete()
        }

    }

}