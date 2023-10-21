package org.chronos.chronostore.api

import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.cursor.Cursor

/**
 * A [TransactionalStore] is a transactional representation of a [Store].
 *
 * A [TransactionalStore] is only available for the lifetime of the [ChronoStoreTransaction] it is bound to. The [Store] has
 * its own lifecycle which is independent of transactions.
 */
interface TransactionalStore {

    /** Indicates if this is a system-internal store (`true`) or a user-defined store (`false`). */
    val isSystemInternal: Boolean
        get() = this.storeId.isSystemInternal

    /** The [StoreId] of this store.*/
    val storeId: StoreId

    /** Whether this store supports data versioning or not.*/
    val isVersioned: Boolean

    /**
     * Returns the latest version of the value bound to the given [key] within the store which is visible to the current [ChronoStoreTransaction].
     *
     * The returned value may be `null` if the key doesn't exist within the store, or the most recent visible operation was a deletion.
     *
     * @param key The key to query.
     *
     * @return The latest value for the key which is visible to the transaction. May be `null`.
     */
    fun getLatest(key: Bytes): Bytes?

    /**
     * A variant of [getLatest] which offers more details about the matching entry.
     *
     * @param key The key to query.
     *
     * @return The latest value for the key which is visible to the transaction, plus additional details.
     */
    fun getLatestWithDetails(key: Bytes): GetResult

    /**
     * Returns the value associated with the given [key] within the store which was valid at the given [timestamp].
     *
     * @param key The key to get the value for.
     * @param timestamp The timestamp at which to perform the query. Must not be negative, and must not exceed the [transaction timestamp][ChronoStoreTransaction.lastVisibleTimestamp].
     *
     * @return The value associated with the given [key] at the given [timestamp]. May be `null` if the key had no value, or if the latest relevant operation was a deletion.
     */
    fun getAtTimestamp(key: Bytes, timestamp: Timestamp): Bytes?

    /**
     * Returns the value associated with the given [key] within the store which was valid at the given [timestamp] plus additional details.
     *
     * @param key The key to get the value for.
     * @param timestamp The timestamp at which to perform the query. Must not be negative, and must not exceed the [transaction timestamp][ChronoStoreTransaction.lastVisibleTimestamp].
     *
     * @return The value associated with the given [key] at the given [timestamp] plus additional details.
     */
    fun getAtTimestampWithDetails(key: Bytes, timestamp: Timestamp): GetResult

    /**
     * Opens a new [Cursor] on the latest visible version of the store.
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
     * Opens a new [Cursor] on the store, reading the data at the given [timestamp].
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
     * Returns `true` if the owning transaction is still open, or `false` if it has been [committed][ChronoStoreTransaction.commit] or [rolled back][ChronoStoreTransaction.rollback].
     */
    val isOpen: Boolean

    companion object {

        /**
         * Opens a new [Cursor] on the latest visible version of the store, executes the [action] on it, and closes the cursor again.
         *
         * The returned cursor **will** contain the (uncommitted) modifications of the current transaction. Modifications applied after opening the cursor will be ignored.
         *
         * This method automatically closes the cursor after the [action] has been performed.
         *
         * @return The result of calling the given [action].
         */
        inline fun <T> TransactionalStore.withCursorOnLatest(action: (Cursor<Bytes, Bytes>) -> T): T {
            return this.openCursorOnLatest().use(action)
        }


        /**
         * Opens a new [Cursor] on the store, reading the data at the given [timestamp]. The given [action] is executed, then the cursor is closed again.
         *
         * Important notes:
         *
         * - The returned cursor will **not** contain the (uncommitted) modifications of the current transaction.
         *
         * - The position of the cursor will **not** be affected by modifications performed by the
         *   current transaction.
         *
         * @return The result of calling the [action] on the cursor.
         */
        inline fun <T> TransactionalStore.withCursorAtTimestamp(timestamp: Timestamp, action: (Cursor<Bytes, Bytes>)->T): T {
            return this.openCursorAtTimestamp(timestamp).use(action)
        }

    }
}