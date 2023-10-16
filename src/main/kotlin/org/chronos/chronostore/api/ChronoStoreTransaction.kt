package org.chronos.chronostore.api

import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId

interface ChronoStoreTransaction : AutoCloseable {

    // =================================================================================================================
    // METADATA
    // =================================================================================================================

    /**
     * The unique ID of this transaction.
     */
    val id: TransactionId

    /**
     * The latest timestamp visible to this transaction.
     */
    val lastVisibleTimestamp: Timestamp

    /**
     * Returns `true` if the transaction is still open, or `false` if it has been [committed][commit] or [rolled back][rollback].
     */
    val isOpen: Boolean

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
    fun getStore(name: StoreId): TransactionBoundStore {
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
    fun getStore(name: String): TransactionBoundStore {
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
    fun getStoreOrNull(name: StoreId): TransactionBoundStore?

    /**
     * Gets an existing store by [name] or `null` if there is no such store.
     *
     * @param name The name of the store to get.
     *
     * @return The store with the given name, or `null` if it doesn't exist.
     *
     * @see getStore
     */
    fun getStoreOrNull(name: String): TransactionBoundStore? {
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
     * Creates a new store with the given [name].
     *
     * Please note that stores may be created immediately, even before the current transaction is [committed][commit],
     * and they may continue to exist even if the current transaction is [rolled back][rollback].
     *
     * @param name The name of the store to create. Must be unique among all stores.
     * @param versioned Use `true` to apply version control on the store and keep old versions of entries,
     * or `false` to discard old versions of entries when they're overwritten.
     *
     * @return The newly created store.
     */
    fun createNewStore(name: StoreId, versioned: Boolean): TransactionBoundStore

    /**
     * Creates a new store with the given [name].
     *
     * Please note that stores may be created immediately, even before the current transaction is [committed][commit],
     * and they may continue to exist even if the current transaction is [rolled back][rollback].
     *
     * @param name The name of the store to create. Must be unique among all stores.
     * @param versioned Use `true` to apply version control on the store and keep old versions of entries,
     * or `false` to discard old versions of entries when they're overwritten.
     *
     * @return The newly created store.
     */
    fun createNewStore(name: String, versioned: Boolean): TransactionBoundStore {
        return this.createNewStore(StoreId.of(name), versioned)
    }

    /**
     * Gets the list of all stores visible to this transaction.
     */
    val allStores: List<TransactionBoundStore>

    // =================================================================================================================
    // LIFECYCLE
    // =================================================================================================================

    /**
     * Commits the transaction.
     *
     * @return The unique timestamp of the commit.
     */
    fun commit(): Timestamp {
        return commit(null)
    }

    /**
     * Commits the transaction.
     *
     * @param metadata The metadata to attach to the commit
     *
     * @return The unique timestamp of the commit.
     */
    fun commit(metadata: Bytes? = null): Timestamp

    /**
     * Rolls back the transaction.
     *
     * This operation will undo all performed modifications (if any) and
     * will forcefully close all cursors which have been opened by this
     * transaction and are still open (if any).
     *
     * After this method has been called, no further queries
     * or modifications on this object will be permitted.
     */
    fun rollback()

    /**
     * Closes the transaction.
     *
     * This is equivalent to calling [rollback].
     */
    override fun close() {
        this.rollback()
    }

}