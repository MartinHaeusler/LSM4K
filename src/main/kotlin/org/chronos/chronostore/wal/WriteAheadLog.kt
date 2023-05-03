package org.chronos.chronostore.wal

import mu.KotlinLogging
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.StreamExtensions.hasNext
import java.io.PushbackInputStream
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class WriteAheadLog(
    val file: VirtualReadWriteFile
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
                WriteAheadLogEntry.writeTransaction(out, walTransaction)
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
                while (input.available() > 0) {
                    val tx = WriteAheadLogEntry.readTransactionFrom(input)
                    check(tx.commitTimestamp > previousTxCommitTimestamp) {
                        "Found non-ascending commit timestamps in the WAL! The WAL file is likely corrupted!"
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
                overWriter.outputStream.buffered().use { outputStream ->
                    this.readWal { transaction ->
                        if (!isTransactionFullyPersisted(transaction)) {
                            // this transaction has not yet been fully persisted outside
                            // the WAL file, so we have to keep the WAL record.
                            WriteAheadLogEntry.writeTransaction(outputStream, transaction)
                        }
                    }
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
                val output = overWriter.outputStream
                this.file.withInputStream { input ->
                    val pushbackInput = PushbackInputStream(input)

                    var currentTransactionStart: WriteAheadLogEntry.BeginTransactionEntry? = null
                    val currentTransactionActions = mutableListOf<WriteAheadLogEntry.ModifyingTransactionEntry>()

                    while (pushbackInput.hasNext()) {
                        when (val entry = WriteAheadLogEntry.readSingleEntryFrom(input)) {
                            is WriteAheadLogEntry.BeginTransactionEntry -> {
                                if (currentTransactionStart != null) {
                                    log.warn {
                                        "Detected incomplete transaction in WAL (Transaction ID '${entry.transactionId}')." +
                                            " Ignoring this transaction."
                                    }
                                }

                                currentTransactionStart = entry
                                currentTransactionActions.clear()
                            }

                            is WriteAheadLogEntry.CommitTransactionEntry -> {
                                if (currentTransactionStart?.transactionId == entry.transactionId) {
                                    // tx completed, keep it
                                    currentTransactionStart.writeTo(output)
                                    for (action in currentTransactionActions) {
                                        action.writeTo(output)
                                    }
                                    entry.writeTo(output)
                                    currentTransactionStart = null
                                    currentTransactionActions.clear()
                                } else {
                                    // out-of-order transaction commit... ignore the entry.
                                    log.warn {
                                        "Detected out-of-order commit entry in WAL (Transaction ID '${entry.transactionId}')." +
                                            " Ignoring this entry."
                                    }
                                }
                            }

                            is WriteAheadLogEntry.ModifyingTransactionEntry -> {
                                if (currentTransactionStart?.transactionId == entry.transactionId) {
                                    // keep the entry
                                    currentTransactionActions.add(entry)
                                } else {
                                    // out-of-order modification... ignore the entry.
                                    log.warn {
                                        "Detected out-of-order change entry in WAL (Transaction ID '${entry.transactionId}')." +
                                            " Ignoring this entry."
                                    }
                                }
                            }
                        }
                    }

                    // at this point, we may have an ongoing transaction which has not received any commit. This may happen
                    // if we were interrupted while writing the WAL file. This is fine; drop these entries by not copying
                    // them to the overWriter.
                    if (currentTransactionStart != null) {
                        log.warn {
                            "Detected incomplete transaction in WAL (Transaction ID '${currentTransactionStart.transactionId}')." +
                                " Ignoring this transaction."
                        }
                    }
                }

                overWriter.commit()
            }
        }
    }

}