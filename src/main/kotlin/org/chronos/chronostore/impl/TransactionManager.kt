package org.chronos.chronostore.impl

import com.google.common.collect.MapMaker
import mu.KotlinLogging
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.api.SystemStore
import org.chronos.chronostore.api.exceptions.ChronoStoreCommitException
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.impl.transaction.ChronoStoreTransactionImpl
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.InverseQualifiedTemporalKey
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import org.chronos.chronostore.wal.WriteAheadLog
import org.chronos.chronostore.wal.WriteAheadLogTransaction
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

        private val log = KotlinLogging.logger {}

    }

    private val openTransactionsLock = ReentrantReadWriteLock(true)
    private val openTransactions = MapMaker().weakValues().makeMap<TransactionId, ChronoStoreTransaction>()

    private val commitLock = ReentrantLock(true)

    @Transient
    private var isOpen = true

    fun createNewTransaction(): ChronoStoreTransaction {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        ChronoStoreStatistics.TRANSACTIONS.incrementAndGet()
        this.openTransactionsLock.write {
            val newTx = ChronoStoreTransactionImpl(
                id = TransactionId.randomUUID(),
                lastVisibleTimestamp = this.timeManager.getNow(),
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
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.commitLock.withLock {
            val commitTimestamp = this.timeManager.getUniqueWallClockTimestamp()
            val walTransaction = tx.toWALTransaction(commitTimestamp, commitMetadata)
            this.storeManager.withStoreReadLock {
                // ensure first that all stores are indeed writeable, otherwise we may
                // write something into our WAL file which isn't actually "legal".
                val storeNameToStore = this.getStoresForTransactionCommit(walTransaction, tx)

                // once we know that all target stores exist, add it to the WAL
                this.writeAheadLog.addCommittedTransaction(walTransaction)

                // we've successfully added the new transaction to the WAL (so it is technically
                // already persistent). Add it to the in-memory stores to make the data available
                // to clients.
                val commitLogStore = this.storeManager.getSystemStore(SystemStore.COMMIT_LOG)

                for ((storeId, commands) in walTransaction.storeIdToCommands.entries) {
                    val store = storeNameToStore.getValue(storeId)
                    // we're missing the changes from this transaction, put them into the store.
                    (store as StoreImpl).tree.putAll(commands)

                    val commitLogCommands = commands.map { createCommitLogEntry(commitTimestamp, storeId, it) }
                    (commitLogStore as StoreImpl).tree.putAll(commitLogCommands)
                }

            }
            log.trace { "Performed commit of transaction ${tx.id}. Transaction timestamp: ${tx.lastVisibleTimestamp}, commit timestamp: ${commitTimestamp}." }
            this.closeTransaction(tx)
            ChronoStoreStatistics.TRANSACTION_COMMITS.incrementAndGet()
            return commitTimestamp
        }
    }

    private fun getStoresForTransactionCommit(
        walTransaction: WriteAheadLogTransaction,
        tx: ChronoStoreTransactionImpl
    ): Map<StoreId, Store> {
        return walTransaction.storeIdToCommands.keys.associateWith { storeId ->
            val store = this.storeManager.getStoreByIdOrNull(tx, storeId)
                ?: throw ChronoStoreCommitException(
                    "Transaction could not be fully committed, because the" +
                        " store '${storeId}' does not exist! Commit will be aborted!"
                )
            if (store.isTerminated) {
                throw ChronoStoreCommitException(
                    "Transaction could not be fully committed, because the" +
                        " store '${storeId}' has been terminated and does not accept" +
                        " any further changes! Commit will be aborted!"
                )
            }
            store
        }
    }

    private fun createCommitLogEntry(
        commitTimestamp: Timestamp,
        storeId: StoreId,
        it: Command
    ): Command {
        val value = when (it.opCode) {
            Command.OpCode.PUT -> Bytes.TRUE
            Command.OpCode.DEL -> Bytes.FALSE
        }
        val inverseQualifiedTemporalKey = InverseQualifiedTemporalKey(commitTimestamp, storeId, it.key)
        return Command.put(inverseQualifiedTemporalKey.toBytes(), commitTimestamp, value)
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