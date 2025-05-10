package org.chronos.chronostore.impl.transaction

import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.api.TransactionalReadWriteStore
import org.chronos.chronostore.api.compaction.CompactionStrategy
import org.chronos.chronostore.impl.TransactionManager
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics

class ChronoStoreTransactionImpl(
    override val id: TransactionId,
    override val lastVisibleSerialNumber: TSN,
    val transactionManager: TransactionManager,
    val storeManager: StoreManager,
    val createdAtWallClockTime: Timestamp,
) : ChronoStoreTransaction {

    private companion object {

        private const val TX_ALREADY_CLOSED = "The transaction has already been closed."

        private val log = KotlinLogging.logger {}

    }

    private val openStoresByName = mutableMapOf<StoreId, TransactionalStoreImpl>()

    @Transient
    override var isOpen: Boolean = true

    init {
        log.trace { "Started transaction ${this.id} at TSN ${lastVisibleSerialNumber}." }
    }

    override fun getStoreOrNull(name: StoreId): TransactionalReadWriteStore? {
        check(this.isOpen) { TX_ALREADY_CLOSED }
        val cachedStore = this.openStoresByName[name]
        if (cachedStore != null) {
            return cachedStore
        }
        val store = this.storeManager.getStoreByIdOrNull(this, name)
            ?: return null

        return this.bindStore(store)
    }

    override fun createNewStore(storeId: StoreId, compactionStrategy: CompactionStrategy?): TransactionalReadWriteStore {
        check(this.isOpen) { TX_ALREADY_CLOSED }
        val newStore = this.storeManager.createNewStore(
            transaction = this,
            storeId = storeId,
            validFromTSN = this.transactionManager.tsnManager.getUniqueTSN(),
            compactionStrategy = compactionStrategy,
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

    override fun commit(): TSN {
        check(this.isOpen) { TX_ALREADY_CLOSED }
        this.isOpen = false

        return this.transactionManager.performCommit(this)
    }

    fun deleteStore(txStore: TransactionalStoreImpl) {
        require(this.isOpen) { TX_ALREADY_CLOSED }
        this.storeManager.deleteStore(this, txStore.storeId)
    }

    val modifiedStoreIds: Set<StoreId>
        get() {
            return this.openStoresByName.entries.asSequence()
                .filter { it.value.transactionContext.isDirty() }
                .map { it.key }
                .toSet()
        }

    fun getChangesAsSequence(commitTSN: TSN): Sequence<Pair<StoreId, Command>> {
        return this.openStoresByName.values.asSequence()
            .filter { it.transactionContext.isDirty() }
            .flatMap { store ->
                store.transactionContext
                    .convertToCommands(commitTSN)
                    .map { store.storeId to it }
            }
    }

    override fun rollback() {
        if (!this.isOpen) {
            return
        }
        this.isOpen = false
        this.transactionManager.performRollback(this)
    }

    private fun bindStore(store: Store): TransactionalStoreImpl {
        val txBoundStore = TransactionalStoreImpl(this, store)
        this.openStoresByName[store.storeId] = txBoundStore
        return txBoundStore
    }

    // the finalizer contains a safety net which could be removed.
    @Suppress("removal")
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