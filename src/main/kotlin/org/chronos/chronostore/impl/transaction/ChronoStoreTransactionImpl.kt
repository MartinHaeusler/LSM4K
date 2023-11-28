package org.chronos.chronostore.impl.transaction

import mu.KotlinLogging
import org.chronos.chronostore.api.*
import org.chronos.chronostore.impl.TransactionManager
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import org.chronos.chronostore.wal.WriteAheadLogTransaction

class ChronoStoreTransactionImpl(
    override val id: TransactionId,
    override val lastVisibleTimestamp: Timestamp,
    val transactionManager: TransactionManager,
    val storeManager: StoreManager,
) : ChronoStoreTransaction {

    private companion object {

        private const val TX_ALREADY_CLOSED = "The transaction has already been closed."

        private val log = KotlinLogging.logger {}

    }

    private val openStoresByName = mutableMapOf<StoreId, TransactionalStoreImpl>()

    @Transient
    override var isOpen: Boolean = true

    init {
        log.trace { "Started transaction ${this.id} at timestamp ${lastVisibleTimestamp}." }
    }

    override fun getStoreOrNull(name: StoreId): TransactionalReadWriteStore? {
        check(this.isOpen) { TX_ALREADY_CLOSED }
        val cachedStore = this.openStoresByName[name]
        if (cachedStore != null) {
            return cachedStore
        }
        val store = this.storeManager.getStoreByNameOrNull(this, name)
            ?: return null

        return this.bindStore(store)
    }

    override fun createNewStore(name: StoreId, versioned: Boolean): TransactionalReadWriteStore {
        check(this.isOpen) { TX_ALREADY_CLOSED }
        val newStore = this.storeManager.createNewStore(
            transaction = this,
            name = name,
            versioned = versioned,
            validFrom = this.transactionManager.timeManager.getUniqueWallClockTimestamp()
        )
        return this.bindStore(newStore)
    }

    override val allStores: List<TransactionalReadWriteStore>
        get() {
            check(this.isOpen) { TX_ALREADY_CLOSED }
            val allStores = this.storeManager.getAllStores(this)
            return allStores.asSequence()
                .filterNot { it.isSystemInternal }
                .map { this.openStoresByName[it.storeId] ?: bindStore(it) }
                .toList()
        }

    override fun commit(metadata: Bytes?): Timestamp {
        check(this.isOpen) { TX_ALREADY_CLOSED }
        this.isOpen = false

        return this.transactionManager.performCommit(this, metadata)
    }

    fun deleteStore(txStore: TransactionalStoreImpl) {
        require(this.isOpen) { TX_ALREADY_CLOSED }
        this.storeManager.deleteStore(this, txStore.storeId)
    }

    fun toWALTransaction(commitTimestamp: Timestamp, metadata: Bytes?): WriteAheadLogTransaction {
        val changes = this.openStoresByName.values.associate { txStore ->
            txStore.store.storeId to txStore.transactionContext.convertToCommands(commitTimestamp)
        }

        return WriteAheadLogTransaction(
            transactionId = this.id,
            commitTimestamp = commitTimestamp,
            storeIdToCommands = changes,
            commitMetadata = metadata ?: Bytes.EMPTY
        )
    }

    override fun rollback() {
        if (!this.isOpen) {
            return
        }
        this.isOpen = false
        log.trace { "Rolled back transaction ${this.id}." }
        ChronoStoreStatistics.TRANSACTION_ROLLBACKS.incrementAndGet()
    }

    private fun bindStore(store: Store): TransactionalStoreImpl {
        val txBoundStore = TransactionalStoreImpl(this, store)
        this.openStoresByName[store.storeId] = txBoundStore
        return txBoundStore
    }

    protected fun finalize() {
        // This is the object finalizer which is called by the JVM garbage collector.
        // If the Garbage Collector encounters a transaction which is no longer referenced but is still
        // open, it means that the programmer who's using the ChronoStore API made some sort of mistake
        // in handling the transactions. Warn them about it.
        if (this.isOpen) {
            log.warn {
                "Dangling transaction (left open, but no longer referenced) was detected and will be rolled back." +
                    " Please ensure that you commit() or rollback() your transactions after using them." +
                    " Use ChronoStore#transaction{ ... } to automate the process."
            }
            ChronoStoreStatistics.TRANSACTION_DANGLING.incrementAndGet()
            this.rollback()
        }
    }
}