package org.chronos.chronostore.impl

import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.impl.transaction.ChronoStoreTransactionImpl
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.wal.WriteAheadLog
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.withLock
import kotlin.concurrent.write

class TransactionManager(
    val storeManager: StoreManager,
    val timeManager: TimeManager,
    val writeAheadLog: WriteAheadLog,
) : AutoCloseable {

    companion object {

        private const val DB_ALREADY_CLOSED = "This Database has already been closed."

    }

    private val openTransactionsLock = ReentrantReadWriteLock(true)
    private val openTransactions = mutableMapOf<TransactionId, ChronoStoreTransaction>()

    private val commitLock = ReentrantLock(true)

    @Transient
    private var isOpen = true

    fun createNewTransaction(): ChronoStoreTransaction {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.openTransactionsLock.write {
            val newTx = ChronoStoreTransactionImpl(
                id = TransactionId.randomUUID(),
                lastVisibleTimestamp = System.currentTimeMillis(),
                storeManager = this.storeManager,
                transactionManager = this,
            )
            this.openTransactions[newTx.id] = newTx
            return newTx
        }
    }


    fun getOpenTransactionIdsAndTimestamps(): Map<TransactionId, Timestamp> {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.openTransactionsLock.read {
            return this.openTransactions.values.associate { it.id to it.lastVisibleTimestamp }
        }
    }

    private fun closeTransaction(tx: ChronoStoreTransaction) {
        this.openTransactionsLock.write {
            this.openTransactions.remove(tx.id)
            tx.close()
        }
    }

    fun performCommit(tx: ChronoStoreTransactionImpl, commitMetadata: Bytes?): Timestamp {
        this.commitLock.withLock {
            val commitTimestamp = this.timeManager.getUniqueWallClockTimestamp()
            val walTransaction = tx.toWALTransaction(commitTimestamp, commitMetadata)
            this.storeManager.withStoreReadLock {
                // ensure first that all stores are indeed writeable, otherwise we may
                // write something into our WAL file which isn't actually "legal".
                val storeIdToStore = walTransaction.storeIdToCommands.keys.asSequence()
                    .mapNotNull { storeId -> this.storeManager.getStoreByIdOrNull(tx, storeId) }
                    .associateBy { it.id }

                this.writeAheadLog.addCommittedTransaction(walTransaction)

                for ((storeId, commands) in walTransaction.storeIdToCommands.entries) {

                    val store = storeIdToStore[storeId]
                        ?: continue // these changes cannot be performed and will be ignored.

                    // TODO: offer a setting to fail commits if some of the target stores don't exist.

                    // we're missing the changes from this transaction,
                    // put them into the store.
                    (store as StoreImpl).tree.put(commands)
                }
            }
            println("Performed commit of transaction. Transaction timestamp: ${tx.lastVisibleTimestamp}, commit timestamp: ${commitTimestamp}.")
            this.closeTransaction(tx)
            return commitTimestamp
        }
    }

    override fun close() {
        if (!this.isOpen) {
            return
        }
        this.isOpen = false
        this.openTransactionsLock.write {
            for (transaction in this.openTransactions.values) {
                transaction.rollback()
            }
        }
    }

}