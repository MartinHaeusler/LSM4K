package org.chronos.chronostore.wal

import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.util.IOExtensions.withInputStream

class WriteAheadLog(
    val file: VirtualReadWriteFile
) {

    @Synchronized
    fun addCommittedTransaction(walTransaction: WriteAheadLogTransaction) {
        this.file.append { out ->
            WriteAheadLogEntry.writeTransaction(out, walTransaction)
        }
    }

    @Synchronized
    fun readWal(consumer: (WriteAheadLogTransaction) -> Unit) {
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