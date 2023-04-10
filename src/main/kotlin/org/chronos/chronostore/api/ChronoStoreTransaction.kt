package org.chronos.chronostore.api

import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.cursor.Cursor

interface ChronoStoreTransaction: AutoCloseable {

    /**
     * The latest timestamp visible to this transaction.
     */
    val lastVisibleTimestamp: Timestamp

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