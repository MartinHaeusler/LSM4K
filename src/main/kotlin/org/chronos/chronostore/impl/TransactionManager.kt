package org.chronos.chronostore.impl

import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.wal.WriteAheadLog
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class TransactionManager(
    val storeManager: StoreManager,
    val timeManager: TimeManager,
    val writeAheadLog: WriteAheadLog,
): AutoCloseable {

    companion object {

        private const val DB_ALREADY_CLOSED = "This Database has already been closed."

    }

    private val lock = ReentrantReadWriteLock(true)
    private val openTransactions = mutableMapOf<TransactionId, ChronoStoreTransaction>()

    @Transient
    private var isOpen = true

    fun createNewTransaction(): ChronoStoreTransaction {
        check(this.isOpen){ DB_ALREADY_CLOSED }
        this.lock.write {
            val newTx = ChronoStoreTransactionImpl(
                id = TransactionId.randomUUID(),
                lastVisibleTimestamp = System.currentTimeMillis()
            )
            this.openTransactions[newTx.id] = newTx
            return newTx
        }
    }


    fun getOpenTransactionIdsAndTimestamps(): Map<TransactionId, Timestamp> {
        check(this.isOpen){ DB_ALREADY_CLOSED }
        this.lock.read {
            return this.openTransactions.values.associate { it.id to it.lastVisibleTimestamp }
        }
    }

    private fun rollbackTransaction(tx: ChronoStoreTransaction) {
        this.lock.write {
            this.openTransactions.remove(tx.id)
        }
    }

    override fun close() {
        if(!this.isOpen){
            return
        }
        this.isOpen = false
        this.lock.write {
            for(transaction in this.openTransactions.values){
                transaction.rollback()
            }
        }
    }

    private inner class ChronoStoreTransactionImpl(
        override val id: TransactionId,
        override val lastVisibleTimestamp: Timestamp
    ) : ChronoStoreTransaction {


        override fun getStoreByNameOrNull(name: String): Store? {
            TODO("Not yet implemented")
        }

        override fun getStoreByIdOrNull(storeId: StoreId): Store? {
            TODO("Not yet implemented")
        }

        override fun createNewStore(name: String, versioned: Boolean): Store {
            TODO("Not yet implemented")
        }

        override fun renameStore(oldName: String, newName: String): Boolean {
            TODO("Not yet implemented")
        }

        override fun renameStore(storeId: StoreId, newName: String): Boolean {
            TODO("Not yet implemented")
        }

        override fun deleteStoreByName(name: String): Boolean {
            TODO("Not yet implemented")
        }

        override fun deleteStoreById(storeId: StoreId): Boolean {
            TODO("Not yet implemented")
        }

        override fun getAllStores(): List<Store> {
            TODO("Not yet implemented")
        }

        override fun put(store: Store, key: Bytes, value: Bytes) {
            TODO("Not yet implemented")
        }

        override fun delete(store: Store, key: Bytes) {
            TODO("Not yet implemented")
        }

        override fun getLatest(store: Store, key: Bytes): Bytes? {
            TODO("Not yet implemented")
        }

        override fun getAtTimestamp(store: Store, key: Bytes, timestamp: Timestamp): Bytes? {
            TODO("Not yet implemented")
        }

        override fun openCursorOnLatest(store: Store): Cursor<Bytes, Bytes> {
            TODO("Not yet implemented")
        }

        override fun openCursorAtTimestamp(store: Store, timestamp: Timestamp): Cursor<Bytes, Bytes> {
            TODO("Not yet implemented")
        }

        override fun commit(metadata: Bytes?): Timestamp {
            TODO("Not yet implemented")
        }

        override fun rollback() {
            TODO("Not yet implemented")
        }

    }


}