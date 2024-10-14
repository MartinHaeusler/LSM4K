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
import org.chronos.chronostore.util.StreamExtensions.hasNext
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.iterator.IteratorExtensions.peekOrNull
import org.chronos.chronostore.util.iterator.IteratorExtensions.toPeekingIterator
import org.chronos.chronostore.util.unit.BinarySize.Companion.Bytes
import org.chronos.chronostore.util.unit.BinarySize.Companion.MiB
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
 * largest transaction TSN which has been (persistently) written by all
 * stores. This is called the "low watermark". If the transaction with the
 * highest TSN in a WAL file is less than or equal to the low watermark,
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
            .sortedBy { it.minTSN }
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
            val targetFile = getOrCreateTargetWALFileForTransactionTSN(walTransaction.commitTSN)
            targetFile.append { out ->
                WriteAheadLogFormat.writeTransaction(walTransaction, this.compressionAlgorithm, out)
            }
        }
    }

    private fun getOrCreateTargetWALFileForTransactionTSN(newTransactionTSN: TSN): WALFile {
        val currentWALFile = this.walFiles.lastOrNull()
        if (currentWALFile != null && !currentWALFile.isFull(this.maxWalFileSizeBytes)) {
            // we still have room in our current file.
            check(currentWALFile.minTSN <= newTransactionTSN) {
                "Cannot write transaction with commit TSN ${newTransactionTSN} into WAL file with min TSN ${currentWALFile.minTSN}!"
            }
            return currentWALFile
        }
        // current WAL file either doesn't exist or is full.
        val newFile = this.directory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}${newTransactionTSN}${WRITE_AHEAD_LOG_FILE_SUFFIX}")
        newFile.create()
        val newWALFile = WALFile(newFile, newTransactionTSN)
        this.walFiles.add(newWALFile)
        return newWALFile
    }

    /**
     * Sequentially reads the Write Ahead Log, permitting a given [consumer] to read the contained transactions one after another.
     *
     * If the file contains invalid entries (e.g. incomplete transactions due to a system shutdown during WAL writes), these
     * entries will **not** be reported to the [consumer]. The consumer will receive the transactions in ascending [TSN] order.
     *
     * @param consumer The consumer function which will receive the transactions in the write-ahead log for further processing.
     *                 The transactions will be provided to the consumer in a lazy fashion, in ascending [TSN] order.
     */
    fun readWalStreaming(consumer: (WriteAheadLogTransaction) -> Unit) {
        this.lock.read {
            var previousTxCommitTSN = -1L
            for ((index, walFile) in this.walFiles.withIndex()) {
                log.debug { "Replaying file '${walFile}' (${walFile.length.Bytes.toHumanReadableString()}) [${index + 1} / ${this.walFiles.size}]" }
                walFile.withInputStream { input ->
                    PushbackInputStream(input).use { pbIn ->
                        while (pbIn.hasNext()) {
                            val tx = WriteAheadLogFormat.readTransaction(pbIn)
                            if (tx.commitTSN <= previousTxCommitTSN) {
                                throw WriteAheadLogEntryCorruptedException("Found non-ascending commit serial number in the WAL! The WAL file is corrupted!")
                            }
                            consumer(tx)
                            previousTxCommitTSN = tx.commitTSN
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
     * @param lowWatermarkTSN The maximum [TSN] for which it is guaranteed that *all* stores have
     *                              persistently stored the results of *all* transactions with commit [TSN]s
     *                              less than or equal to the watermark.
     */
    fun shorten(lowWatermarkTSN: TSN) {
        this.lock.write {
            if (!this.needsToBeShortened()) {
                return
            }
            log.debug { "Attempt to shorten Write-Ahead-Log. Low Watermark: ${lowWatermarkTSN}" }
            val iterator = this.walFiles.iterator().toPeekingIterator()
            val filesToDelete = mutableSetOf<WALFile>()
            while (iterator.hasNext()) {
                val currentWALFile = iterator.next()
                val nextWALFile = iterator.peekOrNull()
                    ?: break // the currentWALFile is actually the LAST file in our WAL, don't drop it.

                if (currentWALFile.minTSN < lowWatermarkTSN && nextWALFile.minTSN < lowWatermarkTSN) {
                    // the current file is below the watermark, because the HIGHEST TSN in this file is strictly
                    // smaller than the min TSN of the next file. Since that is below the watermark, we DEFINITELY
                    // are below the watermark.
                    filesToDelete += currentWALFile
                }
            }
            if (filesToDelete.size > 0) {
                log.debug { "Identified ${filesToDelete.size} Write-Ahead-Log files which can be dropped." }
                this.walFiles.removeAll(filesToDelete)
                filesToDelete.forEach(WALFile::delete)
            } else {
                log.debug { "No Write-Ahead-Log files can be dropped." }
            }
        }
    }

    /**
     * Performs the startup recovery on the Write Ahead Log file.
     *
     * In particular, this method will remove any invalid or incomplete transactions from the WAL.
     */
    fun performStartupRecoveryCleanup(getHighWatermarkTSN: () -> TSN) {
        this.lock.write {
            log.debug { "Checking Write-Ahead-Log for incomplete transactions and data corruption." }
            val timeBefore = System.currentTimeMillis()
            for ((index, walFile) in this.walFiles.withIndex()) {
                log.debug { "Checking file '${walFile.file.name}' (${walFile.file.length.Bytes.toHumanReadableString()}) [${index + 1} / ${this.walFiles.size}]" }
                // we tolerate incomplete trailing writes on the last file.
                val isLastFile = index == this.walFiles.lastIndex

                // Perform a checksum validation if we have a *.md5 file available.
                // This will be much faster than actual content checking but equally effective.
                if (walFile.validateChecksum() == true) {
                    // we can skip this file, the checksum is valid
                    continue
                }

                // we either HAVE no checksum file or it's invalid => check the content.
                walFile.withInputStream { input ->
                    PushbackInputStream(input.buffered()).use { pbIn ->
                        CountingInputStream(pbIn).use { cIn ->
                            var startOfBlock: Long
                            var lastSuccessfulTransactionTSN = -1L
                            while (pbIn.hasNext()) {
                                startOfBlock = cIn.count
                                try {
                                    val tx = WriteAheadLogFormat.readTransaction(cIn)
                                    lastSuccessfulTransactionTSN = tx.commitTSN
                                } catch (e: TruncatedInputException) {
                                    if (isLastFile) {
                                        // the last file in our WAL may become truncated if the last
                                        // commit was interrupted by process kill or power outage. We
                                        // can tolerate that, as long as no store has ever seen a
                                        // higher TSN than the previous entry.
                                        if (getHighWatermarkTSN() > lastSuccessfulTransactionTSN) {
                                            throw WriteAheadLogEntryCorruptedException("WAL file '${walFile.file.path}' is has been truncated and cannot be read!")
                                        }
                                        // truncate the file
                                        log.debug { "Detected truncation of Write-Ahead-Log file '${walFile.file.name}'. This is the latest file and it can be repaired. Dropping partial transactions..." }
                                        walFile.file.truncateAfter(startOfBlock)
                                        log.debug { "Repair of Write-Ahead-Log file '${walFile.file.name}' complete." }
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
            val timeAfter = System.currentTimeMillis()
            log.debug { "Write-Ahead-Log validation complete. Checked ${walFiles.size} files in ${timeAfter - timeBefore}ms." }
        }
    }

    fun generateChecksumsForCompletedFiles() {
        // we do this outside of any lock to avoid blocking writers;
        // technically we're only computing checksums on immutable files so there's
        // not much that can go wrong. Worst case is that we generate a checksum
        // for a file that's being deleted as we compute the checksum.

        // grab a copy of the WAL files list
        val walFiles = this.lock.read { this.walFiles.toMutableList() }
        if (walFiles.isEmpty()) {
            // we don't have any WAL files yet, nothing to do.
            return
        }

        // just a safeguard: sort the WAL files ascending by min TSN. This
        // *should* already be the case anyway. Since we're constantly shortening
        // the WAL again, this list should not have thousands of entries, so the
        // overhead for sorting should be negligible.
        walFiles.sortBy { it.minTSN }

        // drop the last file from the list because this is the "active" WAL
        // which is not complete yet; no point in computing a checksum for it.
        walFiles.removeLast()

        // for all other files, we generate checksums.
        for (walFile in walFiles) {
            walFile.createChecksumFileIfNecessary()
        }
    }

}