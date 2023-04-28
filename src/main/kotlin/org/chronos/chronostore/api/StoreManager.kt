package org.chronos.chronostore.api

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.util.StoreId

interface StoreManager {

    fun getStoreByName(transaction: ChronoStoreTransaction, name: String): Store {
        return getStoreByNameOrNull(transaction, name)
            ?: throw IllegalArgumentException("There is no store with name '${name}'!")
    }

    fun getStoreByNameOrNull(transaction: ChronoStoreTransaction, name: String): Store?

    fun getStoreById(transaction: ChronoStoreTransaction, storeId: StoreId): Store {
        return getStoreByIdOrNull(transaction, storeId)
            ?: throw IllegalArgumentException("There is no store with ID '${storeId}'!")
    }

    fun getStoreByIdOrNull(transaction: ChronoStoreTransaction, storeId: StoreId): Store?

    fun existsStoreByName(transaction: ChronoStoreTransaction, name: String): Boolean {
        return this.getStoreByNameOrNull(transaction, name) != null
    }

    fun existsStoreById(transaction: ChronoStoreTransaction, storeId: StoreId): Boolean {
        return this.getStoreByIdOrNull(transaction, storeId) != null
    }

    fun createNewStore(transaction: ChronoStoreTransaction, name: String, versioned: Boolean): Store

    fun renameStore(transaction: ChronoStoreTransaction, oldName: String, newName: String): Boolean

    fun renameStore(transaction: ChronoStoreTransaction, storeId: StoreId, newName: String): Boolean

    fun deleteStoreByName(transaction: ChronoStoreTransaction, name: String): Boolean

    fun deleteStoreById(transaction: ChronoStoreTransaction, storeId: StoreId): Boolean

    fun getAllStores(transaction: ChronoStoreTransaction): List<Store>

    fun <T> withStoreReadLock(action: () -> T): T

    fun performGarbageCollection(monitor: TaskMonitor)

}