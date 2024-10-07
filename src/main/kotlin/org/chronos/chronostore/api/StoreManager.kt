package org.chronos.chronostore.api

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.Timestamp

interface StoreManager {

    fun getStore(transaction: ChronoStoreTransaction, name: StoreId): Store {
        return getStoreByIdOrNull(transaction, name)
            ?: throw IllegalArgumentException("There is no store with name '${name}'!")
    }

    fun getStoreByIdOrNull(transaction: ChronoStoreTransaction, name: StoreId): Store?

    fun existsStore(transaction: ChronoStoreTransaction, name: StoreId): Boolean {
        return this.getStoreByIdOrNull(transaction, name) != null
    }

    fun getHighWatermarkTSN(): TSN

    fun getLowWatermarkTSN(): TSN

    fun createNewStore(transaction: ChronoStoreTransaction, name: StoreId, validFromTSN: TSN): Store

    fun deleteStore(transaction: ChronoStoreTransaction, name: StoreId): Boolean

    fun getAllStores(transaction: ChronoStoreTransaction): List<Store>

    fun getAllLsmTrees(): List<LSMTree>

    fun <T> withStoreReadLock(action: () -> T): T

    fun performGarbageCollection(monitor: TaskMonitor)

    fun getSystemStore(systemStore: SystemStore): Store

}