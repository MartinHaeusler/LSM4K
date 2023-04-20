package org.chronos.chronostore.api

import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.cursor.Cursor

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
     * @see getStoreByNameOrNull
     */
    fun getStoreByName(name: String): Store {
        return getStoreByNameOrNull(name)
            ?: throw IllegalArgumentException("There is no store with name '${name}'!")
    }

    /**
     * Gets an existing store by [name] or `null` if there is no such store.
     *
     * @param name The name of the store to get.
     *
     * @return The store with the given name, or `null` if it doesn't exist.
     *
     * @see getStoreByName
     */
    fun getStoreByNameOrNull(name: String): Store?

    /**
     * Gets a store by its [storeId].
     *
     * Throws an [IllegalArgumentException] if there is no store with the given [storeId].
     *
     * @param storeId The ID of the store to get.
     *
     * @return The store with the given [storeId].
     *
     * @throws IllegalArgumentException if three is no store with the given [storeId].
     *
     * @see getStoreByIdOrNull
     */
    fun getStoreById(storeId: StoreId): Store {
        return getStoreByIdOrNull(storeId)
            ?: throw IllegalArgumentException("There is no store with ID '${storeId}'!")
    }

    /**
     * Gets a store by its [storeId] or `null` if there is no such store.
     *
     * @param storeId The ID of the store to get.
     *
     * @return The store with the given [storeId], or `null` if it doesn't exist.
     *
     * @see getStoreById
     */
    fun getStoreByIdOrNull(storeId: StoreId): Store?

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
    fun existsStoreByName(name: String): Boolean {
        return this.getStoreByNameOrNull(name) != null
    }

    /**
     * Checks if there is a store with the given [id].
     *
     * @return `true` if there is a store with the given ID, otherwise `false`.
     */
    fun existsStoreById(storeId: StoreId): Boolean {
        return this.getStoreByIdOrNull(storeId) != null
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
    fun createNewStore(name: String, versioned: Boolean): Store

    /**
     * Renames a store.
     *
     * Please note that stores may be renamed immediately, even before the current transaction is [committed][commit],
     * and they may continue to use the new name even if the current transaction is [rolled back][rollback].
     *
     * Versioned stores will follow the same semantics as non-versioned stores, so renaming a versioned store is
     * not recommended.
     *
     * @param oldName The old (current) name of the store.
     * @param newName The new name for the store. The new name must not be in use by another store.
     *
     * @return `true` if the operation was successful, or `false` if there was no store with the given [oldName].
     *
     * @throws IllegalArgumentException if the [newName] is already in use by another store.
     */
    fun renameStore(oldName: String, newName: String): Boolean

    /**
     * Renames a store by [storeId].
     *
     * Please note that stores may be renamed immediately, even before the current transaction is [committed][commit],
     * and they may continue to use the new name even if the current transaction is [rolled back][rollback].
     *
     * Versioned stores will follow the same semantics as non-versioned stores, so renaming a versioned store is
     * not recommended.
     *
     * @param storeId The unique ID of the store.
     * @param newName The new name for the store. The new name must not be in use by another store.
     *
     * @return `true` if the operation was successful, or `false` if there was no store with the given [storeId].
     *
     * @throws IllegalArgumentException if the [newName] is already in use by another store.
     */
    fun renameStore(storeId: StoreId, newName: String): Boolean

    /**
     * Deletes the store with the given [name].
     *
     * The deletion on disk will not be carried out immediately; it will eventually be deleted once all
     * concurrent transactions have been completed. However, the store will immediately be terminated,
     * i.e. any further data changes to the store (including by this transaction) will be rejected with
     * an exception.
     *
     * Please note that versioned stores will continue to exist even after they have been deleted in
     * order to preserve the history.
     *
     * @param name The name of the store to delete.
     *
     * @return `true` if the store was terminated, `false` if no store exists for the given [name].
     */
    fun deleteStoreByName(name: String): Boolean

    /**
     * Deletes the store with the given [storeId].
     *
     * The deletion on disk will not be carried out immediately; it will eventually be deleted once all
     * concurrent transactions have been completed. However, the store will immediately be terminated,
     * i.e. any further data changes to the store (including by this transaction) will be rejected with
     * an exception.
     *
     * Please note that versioned stores will continue to exist even after they have been deleted in
     * order to preserve the history.
     *
     * @param storeId The unique ID of the store to delete.
     *
     * @return `true` if the store was terminated, `false` if no store exists for the given [storeId].
     */
    fun deleteStoreById(storeId: StoreId): Boolean

    /**
     * Gets the list of all stores visible to this transaction.
     */
    fun getAllStores(): List<Store>

    // =================================================================================================================
    // DATA MANAGEMENT
    // =================================================================================================================

    /**
     * Associates the given [key] with the given [value] in the given [store].
     *
     * @param store The store to modify.
     * @param key The key to associate the new [value] with.
     * @param value The value to associate with the [key].
     */
    fun put(store: Store, key: Bytes, value: Bytes)

    /**
     * Deletes the given [key] in the given [store].
     *
     * @param store The store to modify.
     * @param key The key to delete.
     */
    fun delete(store: Store, key: Bytes)

    /**
     * Gets the latest version of the value belonging to the given key.
     *
     * This method also considers transient modifications within this transaction
     * which have been performed via [put] and [delete].
     *
     * @param store The store to query.
     * @param key The key to get the value for.
     *
     * @return The latest value associated with the given [key]. If the last operation on the key was a deletion, `null` will be returned instead.
     */
    fun getLatest(store: Store, key: Bytes): Bytes?

    /**
     * Gets the latest version of the value belonging to the given [key] which is visible at the given [timestamp].
     *
     * This method does **not** consider transient modifications within this transaction.
     *
     * @param store The store to query.
     * @param key The key to get the value for.
     * @param timestamp The timestamp to get the value at.
     *
     * @return The latest value associated with the given [key] at the given [timestamp]. If the last operation on the key was a deletion, `null` will be returned instead.
     */
    fun getAtTimestamp(store: Store, key: Bytes, timestamp: Timestamp): Bytes?

    /**
     * Opens a new [Cursor] which iterates over the entries in the latest version of the given [store].
     *
     * This method also considers transient modifications within this transaction
     * which have been performed via [put] and [delete].
     *
     * Cursors which are still open when the transaction is [closed][rollback] will be closed forcefully.
     *
     * @param store The store to iterate over.
     *
     * @return The cursor. Needs to be [closed][Cursor.close].
     */
    fun openCursorOnLatest(store: Store): Cursor<Bytes, Bytes>

    /**
     * Opens a new [Cursor] which iterates over the entries in the latest version of the given [store] which is visible at the given [timestamp].
     *
     * This method does **not** consider transient modifications within this transaction.
     *
     * Cursors which are still open when the transaction is [closed][rollback] will be closed forcefully.
     *
     * @param store The store to iterate over.
     * @param timestamp The timestamp to query.
     *
     * @return The cursor. Needs to be [closed][Cursor.close].
     */
    fun openCursorAtTimestamp(store: Store, timestamp: Timestamp): Cursor<Bytes, Bytes>

    // =================================================================================================================
    // LIFECYCLE
    // =================================================================================================================

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