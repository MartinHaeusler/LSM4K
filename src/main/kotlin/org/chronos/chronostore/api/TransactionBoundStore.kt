package org.chronos.chronostore.api

import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.cursor.Cursor

/**
 * A [TransactionBoundStore] is a helper class owned by a [transaction] which allows to interact with the bound [store].
 *
 * A [TransactionBoundStore] is only available for the lifetime of the [transaction] it is bound to. The [Store] has
 * its own lifecycle which is independent of transactions.
 */
interface TransactionBoundStore {

    /** The [Store] to which this object refers.*/
    val store: Store

    /** The [ChronoStoreTransaction] to which this object is bound.*/
    val transaction: ChronoStoreTransaction

    /**
     * Inserts the given [key]-[value] pair into the store (in the context of the [transaction]).
     *
     * The change will be visible only to the current [transaction] until it is [committed][ChronoStoreTransaction.commit].
     *
     * @param key The key to modify. Keys are unique within each [store]. If the key already exists in the store, its value will be overwritten.
     * @param value The value to set for the [key].
     */
    fun put(key: Bytes, value: Bytes)

    /**
     * Deletes the given [key] from the [store] (in the context of the [transaction]).
     *
     * The change will be visible only to the current [transaction] until it is [committed][ChronoStoreTransaction.commit].
     *
     * @param key The key to delete.
     */
    fun delete(key: Bytes)

    /**
     * Returns the latest version of the value bound to the given [key] within the [store] which is visible to the current [transaction].
     *
     * The returned value may be `null` if the key doesn't exist within the store, or the most recent visible operation was a [delete].
     *
     * @param key The key to query.
     *
     * @return The latest value for the key which is visible to the transaction. May be `null`.
     */
    fun getLatest(key: Bytes): Bytes?

    /**
     * Returns the value associated with the given [key] within the [store] which was valid at the given [timestamp].
     *
     * @param key The key to get the value for.
     * @param timestamp The timestamp at which to perform the query. Must not be negative, and must not exceed the [transaction timestamp][ChronoStoreTransaction.lastVisibleTimestamp].
     *
     * @return The value associated with the given [key] at the given [timestamp]. May be `null` if the key had no value, or if the latest relevant operation was a [delete].
     */
    fun getAtTimestamp(key: Bytes, timestamp: Timestamp): Bytes?

    /**
     * Opens a new [Cursor] on the latest visible version of the [store].
     *
     * Important notes:
     *
     * - The returned cursor must be [manually closed][Cursor.close].
     *
     * - The returned cursor **will** contain the (uncommitted) modifications of the current transaction. Modifications applied after opening the cursor will be ignored.
     *
     * The recommended usage pattern is to use the [use][AutoCloseable.use] method:
     *
     * ```
     * txStore.openCursorOnLatest().use { cursor ->
     *     // ... use the cursor here ...
     * }
     * ```
     *
     * @return The newly created cursor on the latest visible version of the store data.
     */
    fun openCursorOnLatest(): Cursor<Bytes, Bytes>

    fun getKeysOnLatestAscending(): List<Bytes> {
        this.openCursorOnLatest().use { cursor ->
            return cursor.listAllKeysAscending()
        }
    }

    fun getKeysOnLatestDescending(): List<Bytes> {
        this.openCursorOnLatest().use { cursor ->
            return cursor.listAllKeysDescending()
        }
    }

    fun getValuesOnLatestAscending(): List<Bytes> {
        this.openCursorOnLatest().use { cursor ->
            return cursor.listAllValuesAscending()
        }
    }

    fun getValuesOnLatestDescending(): List<Bytes> {
        this.openCursorOnLatest().use { cursor ->
            return cursor.listAllValuesDescending()
        }
    }

    fun getEntriesOnLatestAscending(): List<Pair<Bytes, Bytes>> {
        this.openCursorOnLatest().use { cursor ->
            return cursor.listAllEntriesAscending()
        }
    }

    fun getEntriesOnLatestDescending(): List<Pair<Bytes, Bytes>> {
        this.openCursorOnLatest().use { cursor ->
            return cursor.listAllEntriesDescending()
        }
    }

    fun getKeysAtTimestampAscending(timestamp: Timestamp): List<Bytes> {
        this.openCursorAtTimestamp(timestamp).use { cursor ->
            return cursor.listAllKeysAscending()
        }
    }

    fun getKeysAtTimestampDescending(timestamp: Timestamp): List<Bytes> {
        this.openCursorAtTimestamp(timestamp).use { cursor ->
            return cursor.listAllKeysDescending()
        }
    }

    fun getValuesAtTimestampAscending(timestamp: Timestamp): List<Bytes> {
        this.openCursorAtTimestamp(timestamp).use { cursor ->
            return cursor.listAllValuesAscending()
        }
    }

    fun getValuesAtTimestampDescending(timestamp: Timestamp): List<Bytes> {
        this.openCursorAtTimestamp(timestamp).use { cursor ->
            return cursor.listAllValuesDescending()
        }
    }

    fun getEntriesAtTimestampAscending(timestamp: Timestamp): List<Pair<Bytes, Bytes>> {
        this.openCursorAtTimestamp(timestamp).use { cursor ->
            return cursor.listAllEntriesAscending()
        }
    }

    fun getEntriesAtTimestampDescending(timestamp: Timestamp): List<Pair<Bytes, Bytes>> {
        this.openCursorAtTimestamp(timestamp).use { cursor ->
            return cursor.listAllEntriesDescending()
        }
    }

    /**
     * Opens a new [Cursor] on the [store], reading the data at the given [timestamp].
     *
     * Important notes:
     *
     * - The returned cursor must be [manually closed][Cursor.close].
     *
     * - The returned cursor will **not** contain the (uncommitted) modifications of the current transaction.
     *
     * - The position of the returned cursor will **not** be affected by modifications performed by the
     *   current transaction.
     *
     * The recommended usage pattern is to use the [use][AutoCloseable.use] method:
     *
     * ```
     * txStore.openCursorOnLatest().use { cursor ->
     *     // ... use the cursor here ...
     * }
     * ```
     *
     * @return The newly created cursor on the store data at the given timestamp.
     */
    fun openCursorAtTimestamp(timestamp: Timestamp): Cursor<Bytes, Bytes>

    /**
     * Renames the [store].
     *
     * Please note that stores may be renamed immediately, even before the current transaction is [committed][ChronoStoreTransaction.commit],
     * and they may continue to use the new name even if the current transaction is [rolled back][ChronoStoreTransaction.rollback].
     *
     * Versioned stores will follow the same semantics as non-versioned stores, so renaming a versioned store is
     * not recommended.
     *
     * @param newName The new name for the store. The new name must not be in use by another store.
     *
     * @throws IllegalArgumentException if the [newName] is already in use by another store.
     */
    fun renameStore(newName: String)

    /**
     * Deletes the [store].
     *
     * The deletion on disk will not be carried out immediately; it will eventually be deleted once all
     * concurrent transactions have been completed. However, the store will immediately be terminated,
     * i.e. any further data changes to the store (including by this transaction) will be rejected with
     * an exception.
     *
     * Please note that versioned stores will continue to exist even after they have been deleted in
     * order to preserve the history.
     */
    fun deleteStore()

    /**
     * Returns `true` if the [transaction] is still open, or `false` if it has been [committed][commit] or [rolled back][rollback].
     */
    val isOpen: Boolean
        get() = this.transaction.isOpen

}