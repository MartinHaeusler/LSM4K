package org.chronos.chronostore.impl.transaction

import mu.KotlinLogging
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.api.TransactionBoundStore
import org.chronos.chronostore.impl.TransactionManager
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.cursor.*
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

    private val openStoresByName = mutableMapOf<String, TransactionBoundStoreImpl>()
    private val openStoresById = mutableMapOf<StoreId, TransactionBoundStoreImpl>()

    @Transient
    override var isOpen: Boolean = true

    init {
        log.debug { "Started transaction ${this.id} at timestamp ${lastVisibleTimestamp}." }
    }

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
        val newStore = this.storeManager.createNewStore(this, name, versioned, this.transactionManager.timeManager.getUniqueWallClockTimestamp())
        return this.bindStore(newStore)
    }

    override val allStores: List<TransactionBoundStore>
        get() {
            check(this.isOpen) { TX_ALREADY_CLOSED }
            val allStores = this.storeManager.getAllStores(this)
            return allStores.asSequence()
                .filterNot { it.isSystemInternal }
                .map { this.openStoresById[it.id] ?: bindStore(it) }
                .toList()
        }

    override fun commit(metadata: Bytes?): Timestamp {
        check(this.isOpen) { TX_ALREADY_CLOSED }
        this.isOpen = false

        return this.transactionManager.performCommit(this, metadata)
    }

    private fun renameStore(txStore: TransactionBoundStoreImpl, newName: String) {
        require(this.isOpen) { TX_ALREADY_CLOSED }
        val oldName = txStore.store.name
        val renamed = this.storeManager.renameStore(this@ChronoStoreTransactionImpl, txStore.store.id, newName)
        if (renamed) {
            this.openStoresByName.remove(oldName)
            this.openStoresByName[oldName] = txStore
        }
    }

    private fun deleteStore(txStore: TransactionBoundStoreImpl) {
        require(this.isOpen) { TX_ALREADY_CLOSED }
        this.storeManager.deleteStoreById(this, txStore.store.id)
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
        log.debug { "Rolled back transaction ${this.id}." }
    }

    private fun bindStore(store: Store): TransactionBoundStoreImpl {
        val txBoundStore = TransactionBoundStoreImpl(store)
        this.openStoresByName[store.name] = txBoundStore
        this.openStoresById[store.id] = txBoundStore
        return txBoundStore
    }

    private inner class TransactionBoundStoreImpl(
        override val store: Store,
    ) : TransactionBoundStore {

        override val transaction: ChronoStoreTransaction
            get() = this@ChronoStoreTransactionImpl

        override fun put(key: Bytes, value: Bytes) {
            this.transactionContext.performPut(key, value)
        }

        override fun delete(key: Bytes) {
            this.transactionContext.performDelete(key)
        }

        override fun getLatest(key: Bytes): Bytes? {
            return if (this.transactionContext.isKeyModified(key)) {
                this.transactionContext.getLatest(key)
            } else {
                val command = (this.store as StoreImpl).tree.get(KeyAndTimestamp(key, this@ChronoStoreTransactionImpl.lastVisibleTimestamp))
                when (command?.opCode) {
                    Command.OpCode.DEL, null -> null
                    Command.OpCode.PUT -> command.value
                }
            }
        }

        override fun getAtTimestamp(key: Bytes, timestamp: Timestamp): Bytes? {
            val now = this@ChronoStoreTransactionImpl.lastVisibleTimestamp
            require(timestamp <= now) {
                "Cannot query key at timestamp ${timestamp}: it is later than the last visible timestamp ($now)!"
            }
            val command = (this.store as StoreImpl).tree.get(KeyAndTimestamp(key, now))
            return when (command?.opCode) {
                Command.OpCode.DEL, null -> null
                Command.OpCode.PUT -> command.value
            }
        }

        override fun openCursorOnLatest(): Cursor<Bytes, Bytes> {
            val treeCursor = (this.store as StoreImpl).tree.openCursor(this@ChronoStoreTransactionImpl)
            val versioningCursor = VersioningCursor(
                treeCursor = treeCursor,
                timestamp = this@ChronoStoreTransactionImpl.lastVisibleTimestamp,
                includeDeletions = false
            )
            val bytesToBytesCursor = versioningCursor.mapValue { it.value }
            val transientModifications = this.transactionContext.allModifications.entries.asSequence().mapNotNull {
                val key = it.key
                val value = it.value
                    ?: return@mapNotNull null
                key to value
            }.toMutableList().sortedBy { it.first }
            val transientModificationCursor = if (transientModifications.isNotEmpty()) {
                IndexBasedCursor(
                    minIndex = 0,
                    maxIndex = transientModifications.lastIndex,
                    getEntryAtIndex = { transientModifications[it] },
                    getCursorName = { "Transient Modification Cursor" }
                )
            } else {
                return bytesToBytesCursor
            }
            return OverlayCursor(bytesToBytesCursor, transientModificationCursor)
        }

        override fun openCursorAtTimestamp(timestamp: Timestamp): Cursor<Bytes, Bytes> {
            val now = this@ChronoStoreTransactionImpl.lastVisibleTimestamp
            require(timestamp in (0..now)) {
                "The given timestamp (${timestamp}) is out of range. Expected a positive" +
                    " number less than or equal to the transaction timestamp (${now})!"
            }
            // transient modifications are ignored on historical queries.
            val treeCursor = (this.store as StoreImpl).tree.openCursor(this@ChronoStoreTransactionImpl)
            val versioningCursor = VersioningCursor(treeCursor, timestamp, false)
            return versioningCursor.mapValue { it.value }
        }

        override fun renameStore(newName: String) {
            this@ChronoStoreTransactionImpl.renameStore(this, newName)
        }

        override fun deleteStore() {
            this@ChronoStoreTransactionImpl.deleteStore(this)
            this.transactionContext.clearModifications()
        }

        val transactionContext = TransactionBoundStoreContext(this.store)

        override fun toString(): String {
            return "TransactionBoundStore[${"${this.store.name} (ID: ${this.store.id})"}]"
        }

    }

}