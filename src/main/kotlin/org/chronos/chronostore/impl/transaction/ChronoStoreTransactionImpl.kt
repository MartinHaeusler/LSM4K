package org.chronos.chronostore.impl.transaction

import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.api.TransactionBoundStore
import org.chronos.chronostore.impl.TimeManager
import org.chronos.chronostore.impl.TransactionManager
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.wal.WriteAheadLogTransaction

class ChronoStoreTransactionImpl(
    override val id: TransactionId,
    override val lastVisibleTimestamp: Timestamp,
    val transactionManager: TransactionManager,
    val storeManager: StoreManager,
) : ChronoStoreTransaction {

    private companion object {

        private const val TX_ALREADY_CLOSED = "The transaction has already been closed."

    }

    private val openStoresByName = mutableMapOf<String, TransactionBoundStoreImpl>()
    private val openStoresById = mutableMapOf<StoreId, TransactionBoundStoreImpl>()

    @Transient
    override var isOpen: Boolean = true

    override fun storeOrNull(name: String): TransactionBoundStore? {
        check(this.isOpen) { TX_ALREADY_CLOSED }
        val cachedStore = this.openStoresByName[name]
        if (cachedStore != null) {
            return cachedStore
        }
        val store = this.storeManager.getStoreByNameOrNull(this, name)
            ?: return null

        return this.bindStore(store)
    }

    override fun storeOrNull(storeId: StoreId): TransactionBoundStore? {
        check(this.isOpen) { TX_ALREADY_CLOSED }
        val cachedStore = this.openStoresById[storeId]
        if (cachedStore != null) {
            return cachedStore
        }
        val store = this.storeManager.getStoreByIdOrNull(this, storeId)
            ?: return null

        return this.bindStore(store)
    }

    override fun createNewStore(name: String, versioned: Boolean): TransactionBoundStore {
        check(this.isOpen) { TX_ALREADY_CLOSED }
        val newStore = this.storeManager.createNewStore(this, name, versioned)
        return this.bindStore(newStore)
    }

    override val allStores: List<TransactionBoundStore>
        get() {
            check(this.isOpen) { TX_ALREADY_CLOSED }
            val allStores = this.storeManager.getAllStores(this)
            return allStores.map { this.openStoresById[it.id] ?: bindStore(it) }
        }

    override fun commit(metadata: Bytes?): Timestamp {
        check(this.isOpen) { TX_ALREADY_CLOSED }
        this.isOpen = false

        return this.transactionManager.performCommit(this, metadata)
    }

    fun toWALTransaction(commitTimestamp: Timestamp, metadata: Bytes?): WriteAheadLogTransaction {
        val changes = this.openStoresById.values.associate { txStore ->
            txStore.store.id to txStore.transactionContext.convertToCommands(commitTimestamp)
        }

        return WriteAheadLogTransaction(this.id, commitTimestamp, changes, metadata ?: Bytes.EMPTY)
    }

    override fun rollback() {
        if (!this.isOpen) {
            return
        }
        this.isOpen = false
        this.closeHandler(this)
    }

    private fun bindStore(store: Store): TransactionBoundStoreImpl {
        val txBoundStore = TransactionBoundStoreImpl(store)
        this.openStoresByName[store.name] = txBoundStore
        this.openStoresById[store.id] = txBoundStore
        return txBoundStore
    }

    private inner class TransactionBoundStoreImpl(
        override val store: Store
    ) : TransactionBoundStore {

        override val transaction: ChronoStoreTransaction
            get() = this@ChronoStoreTransactionImpl

        val transactionContext = TransactionBoundStoreContext()

    }

}