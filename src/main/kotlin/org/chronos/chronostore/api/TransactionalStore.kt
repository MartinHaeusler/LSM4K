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

    /**
     * Returns the value bound to the given [key] within the store.
     *
     * The returned value may be `null` if the key doesn't exist within the store.
     *
     * @param key The key to query.
     *
     * @return The value for the key. May be `null`.
     */
    fun get(key: Bytes): Bytes?

    /**
     * Returns the values for the given [keys] within the store.
     *
     * The returned map is a mapping from requested key to stored value.
     *
     * Please note that any key in the [requested keys][keys] which did **not** exist in the store
     * will **not** have a value in the result map.
     *
     * @param keys The keys to query.
     *
     * @return A mapping from requested keys to the corresponding values. If there is no
     * value for a requested key, it will be absent from the response map.
     */
    fun getMultiple(keys: Iterable<Bytes>): Map<Bytes, Bytes>

    /**
     * A variant of [get] which offers more details about the matching entry.
     *
     * @param key The key to query.
     *
     * @return The value for the key, plus additional details.
     */
    fun getWithDetails(key: Bytes): GetResult

    /**
     * Opens a new [Cursor] on the latest visible version of the store.
     *
     * Important notes:
     *
     * - The returned cursor must be [manually closed][Cursor.close].
     *
     * - The returned cursor **will** contain the (uncommitted) modifications of the current transaction. Modifications applied after opening the cursor will be ignored.
     *
     * The recommended usage pattern is to use the `.use{ ... }` method:
     *
     * ```
     * txStore.openCursorOnLatest().use { cursor ->
     *     // ... use the cursor here ...
     * }
     * ```
     *
     * Generally, users should prefer [withCursor] over using this method directly.
     *
     * @return The newly created cursor on the latest visible version of the store data.
     *
     * @see withCursor
     */
    fun openCursor(): Cursor<Bytes, Bytes>

    /**
     * Returns `true` if the owning transaction is still open, or `false` if it has been [committed][ChronoStoreTransaction.commit] or [rolled back][ChronoStoreTransaction.rollback].
     */
    val isOpen: Boolean

    fun getKeysAscending(): List<Bytes> {
        this.openCursor().use { cursor ->
            return cursor.listAllKeysAscending()
        }
    }

    fun getKeysDescending(): List<Bytes> {
        this.openCursor().use { cursor ->
            return cursor.listAllKeysDescending()
        }
    }

    fun getValuesAscending(): List<Bytes> {
        this.openCursor().use { cursor ->
            return cursor.listAllValuesAscending()
        }
    }

    fun getValuesDescending(): List<Bytes> {
        this.openCursor().use { cursor ->
            return cursor.listAllValuesDescending()
        }
    }

    fun getEntriesAscending(): List<Pair<Bytes, Bytes>> {
        this.openCursor().use { cursor ->
            return cursor.listAllEntriesAscending()
        }
    }

    fun getEntriesDescending(): List<Pair<Bytes, Bytes>> {
        this.openCursor().use { cursor ->
            return cursor.listAllEntriesDescending()
        }
    }

    companion object {

        /**
         * Opens a new [Cursor] on the store, executes the [action] on it, and closes the cursor again.
         *
         * The returned cursor **will** contain the (uncommitted) modifications of the current transaction. Modifications applied after opening the cursor will be ignored.
         *
         * This method automatically closes the cursor after the [action] has been performed.
         *
         * @return The result of calling the given [action].
         */
        inline fun <T> TransactionalStore.withCursor(action: (Cursor<Bytes, Bytes>) -> T): T {
            return this.openCursor().use(action)
        }

    }
}