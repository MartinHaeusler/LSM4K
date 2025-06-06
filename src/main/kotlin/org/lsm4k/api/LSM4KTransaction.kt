package org.lsm4k.api

import org.lsm4k.api.compaction.CompactionStrategy
import org.lsm4k.util.StoreId
import org.lsm4k.util.TSN
import org.lsm4k.util.TransactionId

/**
 * A transaction on a [DatabaseEngine] instance.
 *
 * Transactions can be opened e.g. via [DatabaseEngine.beginReadWriteTransaction] and need to be [closed][close] manually.
 *
 * There are three ways to close a transaction:
 *
 * - [commit]
 *   closes the transaction and atomically applies all of its modifications to the store in an all-or-nothing fashion.
 *   When the call to [commit] returns successfully, the modifications within the transaction are guaranteed
 *   to be persistent on disc.
 *
 * - [rollback]
 *   closes the transaction and discards any changes.
 *
 * - [close]
 *   is for convenience and conformance with [AutoCloseable]. It is equivalent to [rollback].
 *
 */
interface LSM4KTransaction : AutoCloseable {

    // =================================================================================================================
    // METADATA
    // =================================================================================================================

    /**
     * The unique ID of this transaction.
     */
    val id: TransactionId

    /**
     * The latest transaction serial number visible to this transaction.
     */
    val lastVisibleSerialNumber: TSN

    /**
     * Returns `true` if the transaction is still open, or `false` if it has been [committed][commit] or [rolled back][rollback].
     */
    val isOpen: Boolean

    /**
     * Returns the [TransactionMode] of this transaction.
     */
    val mode: TransactionMode

    // =================================================================================================================
    // STORE MANAGEMENT
    // =================================================================================================================

    /**
     * Gets an existing store by [name].
     *
     * Throws an [IllegalArgumentException] if there is no store with the given [name].
     *
     * @param name The name of the store to get.
     *
     * @return The store with the given name.
     *
     * @throws IllegalArgumentException if there is no store with the given [name].
     *
     * @see getStoreOrNull
     */
    fun getStore(name: StoreId): TransactionalReadWriteStore {
        return getStoreOrNull(name)
            ?: throw IllegalArgumentException("There is no store with name '${name}'!")
    }

    /**
     * Gets an existing store by [name].
     *
     * Throws an [IllegalArgumentException] if there is no store with the given [name].
     *
     * @param name The name of the store to get.
     *
     * @return The store with the given name.
     *
     * @throws IllegalArgumentException if there is no store with the given [name].
     *
     * @see getStoreOrNull
     */
    fun getStore(name: String): TransactionalReadWriteStore {
        return getStore(StoreId.of(name))
    }

    /**
     * Gets an existing store by [name] or `null` if there is no such store.
     *
     * @param name The name of the store to get.
     *
     * @return The store with the given name, or `null` if it doesn't exist.
     *
     * @see getStore
     */
    fun getStoreOrNull(name: StoreId): TransactionalReadWriteStore?

    /**
     * Gets an existing store by [name] or `null` if there is no such store.
     *
     * @param name The name of the store to get.
     *
     * @return The store with the given name, or `null` if it doesn't exist.
     *
     * @see getStore
     */
    fun getStoreOrNull(name: String): TransactionalReadWriteStore? {
        return getStoreOrNull(StoreId.of(name))
    }

    /**
     * Checks if there is a store with the given [name].
     *
     * Please note that [createNewStore] may still throw an exception even if
     * this method returns `false` for the same [name]. This is because a
     * concurrent transaction may have created a store with this name, and
     * that store might not be visible to this transaction. Likewise, a
     * versioned store may be terminated but never removed, so its name stays bound.
     *
     * @return `true` if there is a store with the given name, otherwise `false`.
     */
    fun existsStore(name: StoreId): Boolean {
        return this.getStoreOrNull(name) != null
    }

    /**
     * Checks if there is a store with the given [name].
     *
     * Please note that [createNewStore] may still throw an exception even if
     * this method returns `false` for the same [name]. This is because a
     * concurrent transaction may have created a store with this name, and
     * that store might not be visible to this transaction. Likewise, a
     * versioned store may be terminated but never removed, so its name stays bound.
     *
     * @return `true` if there is a store with the given name, otherwise `false`.
     */
    fun existsStore(name: String): Boolean {
        return this.existsStore(StoreId.of(name))
    }

    /**
     * Creates a new store with the given [storeId].
     *
     * Please note that stores may be created immediately, even before the current transaction is [committed][commit],
     * and they may continue to exist even if the current transaction is [rolled back][rollback].
     *
     * @param storeId            The ID of the store to create. Must be unique among all stores.
     * @param compactionStrategy The compaction strategy to use for the new store.
     *   Use `null` to apply the [LSM4KConfiguration.defaultCompactionStrategy] defined in the [LSM4KConfiguration].
     *
     * @return The newly created store.
     */
    fun createNewStore(storeId: StoreId, compactionStrategy: CompactionStrategy? = null): TransactionalReadWriteStore

    /**
     * Creates a new store with the given [name].
     *
     * Please note that stores may be created immediately, even before the current transaction is [committed][commit],
     * and they may continue to exist even if the current transaction is [rolled back][rollback].
     *
     * @param name The name of the store to create. Must be unique among all stores.
     * @param compactionStrategy The compaction strategy to use for the new store.
     *   Use `null` to apply the [LSM4KConfiguration.defaultCompactionStrategy] defined in the [LSM4KConfiguration].
     *
     * @return The newly created store.
     */
    fun createNewStore(name: String, compactionStrategy: CompactionStrategy? = null): TransactionalReadWriteStore {
        return this.createNewStore(StoreId.of(name), compactionStrategy)
    }

    /**
     * Gets the list of all stores visible to this transaction.
     */
    val allStores: List<TransactionalReadWriteStore>

    // =================================================================================================================
    // LIFECYCLE
    // =================================================================================================================

    /**
     * Commits the transaction.
     *
     * Calling [commit] on a transaction which [is closed][isOpen] will
     * throw an [IllegalStateException].
     *
     * @return The unique [TSN] of the commit.
     */
    fun commit(): TSN

    /**
     * Rolls back the transaction.
     *
     * This operation will undo all performed modifications (if any) and
     * will forcefully close all cursors which have been opened by this
     * transaction and are still open (if any).
     *
     * After this method has been called, no further queries
     * or modifications on this object will be permitted.
     *
     * Calling [rollback] on a transaction which [is closed][isOpen] has no effect.
     */
    fun rollback()

    /**
     * Closes the transaction.
     *
     * This is equivalent to calling [rollback].
     *
     * Calling [close] on a transaction which [is closed][isOpen] has no effect.
     */
    override fun close() {
        this.rollback()
    }

}