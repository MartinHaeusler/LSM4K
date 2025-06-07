package io.github.martinhaeusler.lsm4k.api

import io.github.martinhaeusler.lsm4k.util.bytes.Bytes

/**
 * A [TransactionalStore] which additionally allows for write operations.
 */
interface TransactionalReadWriteStore : TransactionalStore {

    /**
     * Inserts the given [key]-[value] pair into the store (in the context of the transaction).
     *
     * The change will be visible only to the current transaction until it is [committed][LSM4KTransaction.commit].
     *
     * @param key The key to modify. Keys are unique within each store. If the key already exists in the store, its value will be overwritten.
     * @param value The value to set for the [key].
     */
    fun put(key: Bytes, value: Bytes)

    /**
     * Deletes the given [key] from the store (in the context of the transaction).
     *
     * The change will be visible only to the current transaction until it is [committed][LSM4KTransaction.commit].
     *
     * @param key The key to delete.
     */
    fun delete(key: Bytes)

    /**
     * Deletes the store.
     *
     * The deletion on disk will not be carried out immediately; it will eventually be deleted once all
     * concurrent transactions have been completed. However, the store will immediately be terminated,
     * i.e. any further data changes to the store (including by this transaction) will be rejected with
     * an exception.
     *
     * Deleting a store does not mean that the data on disk will be cleaned up immediately, nor does
     * it entail that its StoreID will become available immediately for reuse.
     */
    fun deleteStore()

}