package org.chronos.chronostore.wal2

import mu.KotlinLogging
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.StreamExtensions.hasNext
import org.chronos.chronostore.wal.WriteAheadLogEntry
import org.chronos.chronostore.wal.WriteAheadLogTransaction
import java.io.PushbackInputStream
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write


/**
 * The [WriteAheadLog2] class manages the Write-Ahead-Log of the store.
 *
 * Each entry of the log (on disk) consists of a **header**, a **body** and a **footer**:
 *
 * ```
 * +--------------------------------------+--------+
 * | Transaction ID                       |        |
 * | Commit Timestamp                     | HEADER |
 * | Commit Metadata                      |        |
 * | Compression Algorithm                |        |
 * +--------------------------------------+--------+
 * | Block 1 Header                       |        |
 * |   Target Store ID                    |  BODY  |
 * +--------------------------------------+        |
 * |     Block 1 Body                     |        |
 * |       Key + Operation                |        |
 * |       Key + Operation                |  BODY  |
 * |       ...                            |        |
 * |       Key + Operation                |        |
 * +--------------------------------------+        |
 * |     Block 1 Footer                   |        |
 * |       Block Checksum                 |  BODY  |
 * +--------------------------------------+        |
 * | Block 2 Header                       |        |
 * |   Target Store ID                    |        |
 * +--------------------------------------+        |
 * |     Block 2 Body                     |        |
 * |       Key + Operation                |  BODY  |
 * |       Key + Operation                |        |
 * |       ...                            |        |
 * |       Key + Operation                |        |
 * +--------------------------------------+        |
 * |     Block 2 Footer                   |  BODY  |
 * |       Block Checksum                 |        |
 * +--------------------------------------+        |
 * |   ...                                |        |
 * +--------------------------------------+        |
 * | Block N Header                       |  BODY  |
 * |   Target Store ID                    |        |
 * +--------------------------------------+        |
 * |     Block N Body                     |        |
 * |       Key + Operation                |  BODY  |
 * |       Key + Operation                |        |
 * |       ...                            |        |
 * |       Key + Operation                |        |
 * +--------------------------------------+        |
 * |     Block N Footer                   |  BODY  |
 * |       Block Checksum                 |        |
 * +--------------------------------------+--------+
 * | Entry Checksum                       | FOOTER |
 * | Magic Byte                           |        |
 * +--------------------------------------+--------+
 * ```
 *
 * It's worth noting that:
 *
 * - The *entire* body is being compressed as one block.
 * - The compression algorithm is contained in the header.
 *   In theory, every block may have its own compression
 *   algorithm. This allows us to change the WAL compression
 *   algorithm over time.
 * - The StoreID of every store is only encoded once, and only
 *   if this store has received changes from the transaction.
 * - The footer contains a checksum for the **compressed**
 *   bytes. This allows us to check for corrupted or truncated
 *   entries. The checksum is non-cryptographic.
 * - The entire entry is terminated by a magic byte. This is
 *   a fixed sequence of 8 bytes which is used as a "checkpoint".
 *   If an entry is not terminated by these magic bytes, it is
 *   considered invalid. This is another safeguard against
 *   truncation.
 * - Each entry is written atomically to the WAL, i.e. no
 *   interleavings between entries are allowed. This means
 *   that this WAL format can only process one transaction
 *   commit at a time, which is fine because we expect the
 *   transaction commit timestamps to be unique.
 */
class WriteAheadLog2(
    val file: VirtualReadWriteFile,
    val compressionAlgorithm: CompressionAlgorithm,
) {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    private val lock = ReentrantReadWriteLock(true)

    fun createFileIfNotExists() {
        this.lock.write {
            if (!this.file.exists()) {
                this.file.create()
            }
        }
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
            this.file.append { out ->
                WriteAheadLogFormat.writeTransaction(walTransaction, this.compressionAlgorithm, out)
            }
        }
    }

    /**
     * Sequentially reads the Write Ahead Log file, permitting a given [consumer] to read the contained transactions one after another.
     *
     * If the file contains invalid entries (e.g. incomplete transactions due to a system shutdown during WAL writes), these
     * entries will **not** be reported to the [consumer]. The consumer will receive the transactions in ascending timestamp order.
     *
     * @param consumer The consumer function which will receive the transactions in the write-ahead log for further processing.
     *                 The transactions will be provided to the consumer in a lazy fashion, in ascending timestamp order.
     */
    fun readWal(consumer: (WriteAheadLogTransaction) -> Unit) {
        this.lock.read {
            this.file.withInputStream { input ->
                var previousTxCommitTimestamp = -1L
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
     * @param isTransactionFullyPersisted A function which can determine if all elements of the given transaction have been fully persisted.
     *                                    Receives the transaction as argument and returns `true` if all changes have been fully persisted
     *                                    (in this case, the transaction will be removed from the WAL), or `false` if some of the changes
     *                                    are only held in-memory (in this case, the transaction will remain in the WAL).
     */
    fun compactWal(isTransactionFullyPersisted: (WriteAheadLogTransaction) -> Boolean) {
        this.lock.write {
            this.file.deleteOverWriterFileIfExists()
            this.file.createOverWriter().use { overWriter ->
                // do NOT close the outputStream obtained by the overWriter!
                // It will be flushed and f-synced when the overWriter is committed.
                overWriter.outputStream.buffered().also { outputStream ->
                    this.readWal { transaction ->
                        if (!isTransactionFullyPersisted(transaction)) {
                            // this transaction has not yet been fully persisted outside
                            // the WAL file, so we have to keep the WAL record.
                            WriteAheadLogFormat.writeTransaction(transaction, this.compressionAlgorithm, outputStream)
                        }
                    }
                    // flush the buffer of the output stream, but don't close it, because
                    // this will fail the fsync in the overWriter.
                    outputStream.flush()
                }
                overWriter.commit()
            }
        }
    }

    /**
     * Performs the startup recovery on the Write Ahead Log file.
     *
     * In particular, this method will remove any invalid or incomplete transactions from the WAL.
     */
    fun performStartupRecoveryCleanup() {
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

}