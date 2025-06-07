package io.github.martinhaeusler.lsm4k.api

import io.github.martinhaeusler.lsm4k.api.TransactionalStore.Companion.withCursor
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.cursor.Cursor

/**
 * A [TransactionalStore] is a transactional representation of a [Store].
 *
 * A [TransactionalStore] is only available for the lifetime of the [LSM4KTransaction] it is bound to. The [Store] has
 * its own lifecycle which is independent of transactions.
 */
interface TransactionalStore {

    companion object {

        /**
         * Opens a new [Cursor] on the store, executes the [action] on it, and closes the cursor again.
         *
         * Java users should prefer [useCursor] instead, as it doesn't use Kotlin's "inline" functionality.
         *
         * The returned cursor **will** contain the (uncommitted) modifications of the current transaction. Modifications applied after opening the cursor will be ignored.
         *
         * This method automatically closes the cursor after the [action] has been performed.
         *
         * @return The result of calling the given [action].
         *
         * @see useCursor
         */
        inline fun <T> TransactionalStore.withCursor(action: (Cursor<Bytes, Bytes>) -> T): T {
            return this.openCursor().use(action)
        }
    }

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
     * Opens a new [Cursor] on the store, executes the [action] on it, and closes the cursor again.
     *
     * The returned cursor **will** contain the (uncommitted) modifications of the current transaction. Modifications applied after opening the cursor will be ignored.
     *
     * This method automatically closes the cursor after the [action] has been performed.
     *
     * **Kotlin Users:** Prefer the extension method `withCursor` instead which is an `inline` function.
     *
     * @return The result of calling the given [action].
     */
    fun <T> useCursor(action: (Cursor<Bytes, Bytes>) -> T): T {
        return this.openCursor().use(action)
    }

    /**
     * Returns `true` if the owning transaction is still open, or `false` if it has been [committed][LSM4KTransaction.commit] or [rolled back][LSM4KTransaction.rollback].
     */
    val isOpen: Boolean

    /**
     * Lists **all** keys in the store in ascending order.
     *
     * **CAUTION:** For large stores, this may result in very large lists!
     * Use [useCursor] (Java) or [withCursor] (Kotlin) instead to fetch the keys iteratively and avoid
     * loading them all into memory at the same time!
     *
     * **PERFORMANCE NOTE:** This method [owns][Bytes.own] all of the [Bytes] which result
     * in lower performance. For maximum performance, use a cursor directly and consume
     * each object immediately without owning it first.
     *
     * @return A list of all keys in the store, in ascending order.
     */
    fun getKeysAscending(): List<Bytes> {
        this.withCursor { cursor ->
            if (!cursor.first()) {
                return emptyList()
            }
            val sequence = cursor.ascendingKeySequenceFromHere(includeCurrent = true)
            return sequence.map { it.own() }.toList()
        }
    }

    /**
     * Lists **all** keys in the store in descending order.
     *
     * **CAUTION:** For large stores, this may result in very large lists!
     * Use [useCursor] (Java) or [withCursor] (Kotlin) instead to fetch the keys iteratively and avoid
     * loading them all into memory at the same time!
     *
     * **PERFORMANCE NOTE:** This method [owns][Bytes.own] all of the [Bytes] which result
     * in lower performance. For maximum performance, use a cursor directly and consume
     * each object immediately without owning it first.
     *
     * @return A list of all keys in the store, in descending order.
     */
    fun getKeysDescending(): List<Bytes> {
        this.withCursor { cursor ->
            if (!cursor.last()) {
                return emptyList()
            }
            val sequence = cursor.descendingKeySequenceFromHere(includeCurrent = true)
            return sequence.map { it.own() }.toList()
        }
    }

    /**
     * Lists **all** values in the store in ascending key order.
     *
     * **CAUTION:** For large stores, this may result in very large lists!
     * Use [useCursor] (Java) or [withCursor] (Kotlin) instead to fetch the keys iteratively and avoid
     * loading them all into memory at the same time!
     *
     * **PERFORMANCE NOTE:** This method [owns][Bytes.own] all of the [Bytes] which result
     * in lower performance. For maximum performance, use a cursor directly and consume
     * each object immediately without owning it first.
     *
     * @return A list of all values in the store, sorted by their corresponding keys (ascending).
     */
    fun getValuesAscending(): List<Bytes> {
        this.withCursor { cursor ->
            if (!cursor.first()) {
                return emptyList()
            }
            val sequence = cursor.ascendingValueSequenceFromHere(includeCurrent = true)
            return sequence.map { it.own() }.toList()
        }
    }

    /**
     * Lists **all** values in the store in descending key order.
     *
     * **CAUTION:** For large stores, this may result in very large lists!
     * Use [useCursor] (Java) or [withCursor] (Kotlin) instead to fetch the keys iteratively and avoid
     * loading them all into memory at the same time!
     *
     * **PERFORMANCE NOTE:** This method [owns][Bytes.own] all of the [Bytes] which result
     * in lower performance. For maximum performance, use a cursor directly and consume
     * each object immediately without owning it first.
     *
     * @return A list of all values in the store, sorted by their corresponding keys (descending).
     */
    fun getValuesDescending(): List<Bytes> {
        this.withCursor { cursor ->
            if (!cursor.last()) {
                return emptyList()
            }
            val sequence = cursor.descendingValueSequenceFromHere(includeCurrent = true)
            return sequence.map { it.own() }.toList()
        }
    }

    /**
     * Lists **all** entries in the store in ascending key order.
     *
     * **CAUTION:** For large stores, this may result in very large lists!
     * Use [useCursor] (Java) or [withCursor] (Kotlin) instead to fetch the keys iteratively and avoid
     * loading them all into memory at the same time!
     *
     * **PERFORMANCE NOTE:** This method [owns][Bytes.own] all of the [Bytes] which result
     * in lower performance. For maximum performance, use a cursor directly and consume
     * each object immediately without owning it first.
     *
     * @return A list of all entries in the store, sorted by their corresponding keys (ascending).
     */
    fun getEntriesAscending(): List<Pair<Bytes, Bytes>> {
        this.withCursor { cursor ->
            if (!cursor.first()) {
                return emptyList()
            }
            val sequence = cursor.ascendingEntrySequenceFromHere(includeCurrent = true)
            return sequence.map { it.own() }.toList()
        }
    }

    /**
     * Lists **all** entries in the store in descending key order.
     *
     * **CAUTION:** For large stores, this may result in very large lists!
     * Use [useCursor] (Java) or [withCursor] (Kotlin) instead to fetch the keys iteratively and avoid
     * loading them all into memory at the same time!
     *
     * **PERFORMANCE NOTE:** This method [owns][Bytes.own] all of the [Bytes] which result
     * in lower performance. For maximum performance, use a cursor directly and consume
     * each object immediately without owning it first.
     *
     * @return A list of all entries in the store, sorted by their corresponding keys (descending).
     */
    fun getEntriesDescending(): List<Pair<Bytes, Bytes>> {
        this.withCursor { cursor ->
            if (!cursor.last()) {
                return emptyList()
            }
            val sequence = cursor.descendingEntrySequenceFromHere(includeCurrent = true)
            return sequence.map { it.own() }.toList()
        }
    }

    /**
     * Performs a prefix scan on the store and returns the matching keys in ascending order.
     *
     * **CAUTION:** For large stores, this may result in very large lists!
     * Use [useCursor] (Java) or [withCursor] (Kotlin) instead to fetch the keys iteratively and avoid
     * loading them all into memory at the same time!
     *
     * **PERFORMANCE NOTE:** This method [owns][Bytes.own] all of the [Bytes] which result
     * in lower performance. For maximum performance, use a cursor directly and consume
     * each object immediately without owning it first.
     *
     * @param keyPrefix The prefix to scan for
     *
     * @return All keys in the store which have the given [keyPrefix], in ascending order.
     */
    fun getKeysWithPrefixAscending(keyPrefix: Bytes): List<Bytes> {
        this.withCursor { cursor ->
            if (!cursor.seekExactlyOrNext(keyPrefix)) {
                // nothing in the store has that prefix
                return emptyList()
            }
            val resultList = mutableListOf<Bytes>()
            while (cursor.key.startsWith(keyPrefix)) {
                resultList += cursor.key.own()
                if (!cursor.next()) {
                    // we're at the end of the store
                    break
                }
            }
            return resultList
        }
    }

    /**
     * Performs a prefix scan on the store and returns the values associated with the matching keys in ascending key order.
     *
     * **CAUTION:** For large stores, this may result in very large lists!
     * Use [useCursor] (Java) or [withCursor] (Kotlin) instead to fetch the keys iteratively and avoid
     * loading them all into memory at the same time!
     *
     * **PERFORMANCE NOTE:** This method [owns][Bytes.own] all of the [Bytes] which result
     * in lower performance. For maximum performance, use a cursor directly and consume
     * each object immediately without owning it first.
     *
     * @param keyPrefix The key prefix to scan for
     *
     * @return All values in the store which are associated with keys that have the given [keyPrefix], in ascending key order.
     */
    fun getValuesWithKeyPrefixAscending(keyPrefix: Bytes): List<Bytes> {
        this.withCursor { cursor ->
            if (!cursor.seekExactlyOrNext(keyPrefix)) {
                // nothing in the store has that prefix
                return emptyList()
            }
            val resultList = mutableListOf<Bytes>()
            while (cursor.key.startsWith(keyPrefix)) {
                resultList += cursor.value.own()
                if (!cursor.next()) {
                    // we're at the end of the store
                    break
                }
            }
            return resultList
        }
    }

    /**
     * Performs a prefix scan on the store and returns the entries with the matching keys in ascending key order.
     *
     * **CAUTION:** For large stores, this may result in very large lists!
     * Use [useCursor] (Java) or [withCursor] (Kotlin) instead to fetch the keys iteratively and avoid
     * loading them all into memory at the same time!
     *
     * **PERFORMANCE NOTE:** This method [owns][Bytes.own] all of the [Bytes] which result
     * in lower performance. For maximum performance, use a cursor directly and consume
     * each object immediately without owning it first.
     *
     * @param keyPrefix The key prefix to scan for
     *
     * @return All entries in the store with keys that have the given [keyPrefix], in ascending key order.
     */
    fun getEntriesWithKeyPrefixAscending(keyPrefix: Bytes): List<Pair<Bytes, Bytes>> {
        this.withCursor { cursor ->
            if (!cursor.seekExactlyOrNext(keyPrefix)) {
                // nothing in the store has that prefix
                return emptyList()
            }
            val resultList = mutableListOf<Pair<Bytes, Bytes>>()
            while (cursor.key.startsWith(keyPrefix)) {
                resultList += Pair(cursor.key.own(), cursor.value.own())
                if (!cursor.next()) {
                    // we're at the end of the store
                    break
                }
            }
            return resultList
        }
    }

    private fun Pair<Bytes, Bytes>.own(): Pair<Bytes, Bytes> {
        return Pair(this.first.own(), this.second.own())
    }
}