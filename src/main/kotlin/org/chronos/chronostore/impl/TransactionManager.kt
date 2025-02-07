package org.chronos.chronostore.impl

import com.google.common.collect.MapMaker
import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.api.exceptions.CommitException
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.impl.transaction.ChronoStoreTransactionImpl
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import org.chronos.chronostore.wal.WriteAheadLog
import org.chronos.chronostore.wal.WriteAheadLogTransaction
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class TransactionManager(
    val storeManager: StoreManager,
    val tsnManager: TSNManager,
    val writeAheadLog: WriteAheadLog,
) : AutoCloseable {

    companion object {

        private const val DB_ALREADY_CLOSED = "This Database has already been closed."

        private val log = KotlinLogging.logger {}

    }

    // we use a map with weak values here because:
    // - If user code opens a transaction...
    // - ... but forgets to close it...
    // - ... and eventually no longer holds a reference to it...
    // - ... then it will disappear from the map and be "cleaned up" automatically.
    // This is important because some cleanup algorithms will look for the open transaction with
    // the smallest "lastVisibleSerialNumber". A dangling transaction may artificially keep this
    // value low for no reason.
    private val openTransactions: ConcurrentMap<TransactionId, ChronoStoreTransaction> = MapMaker()
        .weakValues()
        .makeMap()

    private val commitLock = ReentrantLock(true)

    @Transient
    private var isOpen = true

    fun createNewTransaction(): ChronoStoreTransaction {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        ChronoStoreStatistics.TRANSACTIONS.incrementAndGet()
        val newTx = ChronoStoreTransactionImpl(
            id = TransactionId.randomUUID(),
            lastVisibleSerialNumber = this.tsnManager.getLastReturnedTSN(),
            storeManager = this.storeManager,
            transactionManager = this,
        )
        this.openTransactions[newTx.id] = newTx
        return newTx
    }


    fun getOpenTransactionIdsAndTSNs(): Map<TransactionId, TSN> {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        return this.openTransactions.values.associate { it.id to it.lastVisibleSerialNumber }
    }

    fun performCommit(tx: ChronoStoreTransactionImpl): TSN {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.commitLock.withLock {
            val commitTSN = this.tsnManager.getUniqueTSN()
            // TODO [SCALABILITY]: The commit may be larger than our main memory (if the transaction spills its context to disk first)
            //                     Therefore we should STREAM the to-be-changed entries from the transaction here directly into the WAL.
            val walTransaction = tx.toWALTransaction(commitTSN)
            this.storeManager.withStoreReadLock {
                // ensure first that all stores are indeed writeable, otherwise we may
                // write something into our WAL file which isn't actually "legal".
                val storeNameToStore = this.getStoresForTransactionCommit(walTransaction, tx)

                // once we know that all target stores exist, add it to the WAL
                this.writeAheadLog.addCommittedTransaction(walTransaction)

                for ((storeId, commands) in walTransaction.storeIdToCommands.entries) {
                    val store = storeNameToStore.getValue(storeId)
                    // we're missing the changes from this transaction, put them into the store.
                    (store as StoreImpl).tree.putAll(commands)
                }

            }
            log.trace { "Performed commit of transaction ${tx.id}. Transaction TSN: ${tx.lastVisibleSerialNumber}, commit TSN: ${commitTSN}." }
            this.openTransactions.remove(tx.id)
            tx.close()
            ChronoStoreStatistics.TRANSACTION_COMMITS.incrementAndGet()
            return commitTSN
        }
    }

    private fun getStoresForTransactionCommit(
        walTransaction: WriteAheadLogTransaction,
        tx: ChronoStoreTransactionImpl,
    ): Map<StoreId, Store> {
        return walTransaction.storeIdToCommands.keys.associateWith { storeId ->
            val store = this.storeManager.getStoreByIdOrNull(tx, storeId)
                ?: throw CommitException(
                    "Transaction could not be fully committed, because the" +
                        " store '${storeId}' does not exist! Commit will be aborted!"
                )
            if (store.isTerminated) {
                throw CommitException(
                    "Transaction could not be fully committed, because the" +
                        " store '${storeId}' has been terminated and does not accept" +
                        " any further changes! Commit will be aborted!"
                )
            }
            store
        }
    }

    override fun close() {
        if (!this.isOpen) {
            return
        }
        this.isOpen = false
        for (transaction in this.openTransactions.values) {
            transaction.rollback()
        }
    }


}