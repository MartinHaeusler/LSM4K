package org.chronos.chronostore.wal2

import mu.KotlinLogging
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.io.structure.ChronoStoreStructure.WRITE_AHEAD_LOG_FILE_PREFIX
import org.chronos.chronostore.io.structure.ChronoStoreStructure.WRITE_AHEAD_LOG_FILE_SUFFIX
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.iterator.IteratorExtensions.peekOrNull
import org.chronos.chronostore.util.iterator.IteratorExtensions.toPeekingIterator
import org.chronos.chronostore.wal.WriteAheadLogTransaction
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write


/**
 * The [WriteAheadLog2] class manages the Write-Ahead-Log (WAL) of the store.
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
 */
class WriteAheadLog2(
    private val directory: VirtualDirectory,
    private val compressionAlgorithm: CompressionAlgorithm,
    private val maxWalFileSizeBytes: Long,
) {

    companion object {

        private val log = KotlinLogging.logger {}

        private val FILE_NAME_REGEX = """$WRITE_AHEAD_LOG_FILE_PREFIX(\d+)$WRITE_AHEAD_LOG_FILE_SUFFIX""".toRegex()

    }

    @Transient
    private var initialized: Boolean = false

    private val lock = ReentrantReadWriteLock(true)

    private val walFiles = mutableListOf<WALFile>()

    init {
        require(maxWalFileSizeBytes > 0) { "Argument 'maxWalFileSizeBytes' (${maxWalFileSizeBytes}) must be positive!" }
    }

    fun initialize() {
        this.lock.write {
            if (initialized) {
                return
            }
            initialized = true
            if (!this.directory.exists()) {
                this.directory.mkdirs()
            }
            this.directory.listFiles().asSequence()
                .mapNotNull(::createWALFileOrNull)
                .sortedBy { it.minTimestamp }
                .forEach(walFiles::add)
        }
    }

    private fun createWALFileOrNull(file: VirtualFile): WALFile? {
        if (file !is VirtualReadWriteFile) {
            return null
        }
        val match = FILE_NAME_REGEX.matchEntire(file.name)
            ?: return null
        val sequenceNumber = match.groups[2]?.value?.toLong()
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
        this.assertInitialized()
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
    fun readWal(consumer: (WriteAheadLogTransaction) -> Unit) {
        this.assertInitialized()
        this.lock.read {
            var previousTxCommitTimestamp = -1L
            for (walFile in this.walFiles) {
                walFile.withInputStream { input ->
                    while (true) {
                        val tx = WriteAheadLogFormat.readTransactionOrNull(input)
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
     * @param lowWatermarkTimestamp The maximum timestamp for which it is guaranteed that *all* stores have
     *                              persistently stored the results of *all* transactions with commit timestamps
     *                              less than or equal to the watermark.
     */
    fun compactWal(lowWatermarkTimestamp: Timestamp) {
        this.assertInitialized()
        this.lock.write {
            val iterator = this.walFiles.iterator().toPeekingIterator()
            while (iterator.hasNext()) {
                val currentWALFile = iterator.next()
                val nextWALFile = iterator.peekOrNull()
                    ?: break // the currentWALFile is actually the LAST file in our WAL, don't drop it.

                if(currentWALFile.minTimestamp < lowWatermarkTimestamp && nextWALFile.minTimestamp < lowWatermarkTimestamp){
                    // the current file is below the watermark, because the HIGHEST timestamp in this file is strictly
                    // smaller than the min timestamp of the next file. Since that is below the watermark, we DEFINITELY
                    // are below the watermark.
                    currentWALFile.delete()
                    iterator.remove()
                }
            }
        }

    }

    /**
     * Performs the startup recovery on the Write Ahead Log file.
     *
     * In particular, this method will remove any invalid or incomplete transactions from the WAL.
     */
    fun performStartupRecoveryCleanup() {
        this.assertInitialized()
        this.lock.write {
            this.file.deleteOverWriterFileIfExists()
            this.file.createOverWriter().use { overWriter ->
                // TODO [PERFORMANCE]: rewriting the entire WAL is quite inefficient.
                val output = overWriter.outputStream
                this.file.withInputStream { input ->
                    while (true) {
                        try {
                            val tx = WriteAheadLogFormat.readTransactionOrNull(input)
                                ?: break
                            WriteAheadLogFormat.writeTransaction(tx, this.compressionAlgorithm, output)
                        } catch (e: WriteAheadLogEntryCorruptedException) {
                            log.warn { "Found corrupted or incomplete WAL entry. Cleaning WAL transactions..." }
                            // entry is corrupted, stop reading
                            break
                        }
                    }
                }
                overWriter.commit()
            }
        }
    }

    @Suppress("NOTHING_TO_INLINE")
    private inline fun assertInitialized() {
        check(this.initialized) { "WriteAheadLog has not been initialized yet!" }
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