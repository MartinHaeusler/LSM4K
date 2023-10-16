package org.chronos.chronostore.api

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp

interface StoreManager {

    fun getStore(transaction: ChronoStoreTransaction, name: StoreId): Store {
        return getStoreByNameOrNull(transaction, name)
            ?: throw IllegalArgumentException("There is no store with name '${name}'!")
    }

    fun getStoreByNameOrNull(transaction: ChronoStoreTransaction, name: StoreId): Store?

    fun existsStore(transaction: ChronoStoreTransaction, name: StoreId): Boolean {
        return this.getStoreByNameOrNull(transaction, name) != null
    }

    fun createNewStore(transaction: ChronoStoreTransaction, name: StoreId, versioned: Boolean, validFrom: Timestamp): Store

    fun deleteStore(transaction: ChronoStoreTransaction, name: StoreId): Boolean

    fun getAllStores(transaction: ChronoStoreTransaction): List<Store>

    fun getAllLsmTrees(): List<LSMTree>

    fun <T> withStoreReadLock(action: () -> T): T

    fun performGarbageCollection(monitor: TaskMonitor)

    fun getSystemStore(systemStore: SystemStore): Store

}