package org.chronos.chronostore.wal

import mu.KotlinLogging
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.StreamExtensions.hasNext
import org.chronos.chronostore.util.TransactionId
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

    fun addCommittedTransaction(walTransaction: WriteAheadLogTransaction) {
        this.lock.write {
            this.file.append { out ->
                WriteAheadLogEntry.writeTransaction(out, walTransaction)
            }
        }
    }

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