package org.chronos.chronostore.api

import org.chronos.chronostore.api.compaction.CompactionStrategy
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TSN

interface StoreManager {

    fun getStore(transaction: ChronoStoreTransaction, name: StoreId): Store {
        return getStoreByIdOrNull(transaction, name)
            ?: throw IllegalArgumentException("There is no store with name '${name}'!")
    }

    fun getStoreByIdOrNull(transaction: ChronoStoreTransaction, name: StoreId): Store?

    fun existsStore(transaction: ChronoStoreTransaction, name: StoreId): Boolean {
        return this.getStoreByIdOrNull(transaction, name) != null
    }

    fun getHighWatermarkTSN(): TSN?

    fun getLowWatermarkTSN(): TSN?

    fun createNewStore(transaction: ChronoStoreTransaction, storeId: StoreId, validFromTSN: TSN, compactionStrategy: CompactionStrategy?): Store

    fun deleteStore(transaction: ChronoStoreTransaction, storeId: StoreId): Boolean

    fun getAllStores(transaction: ChronoStoreTransaction): List<Store>

    fun <T> withStoreReadLock(action: () -> T): T

    fun performGarbageCollection(monitor: TaskMonitor)

    fun getSystemStore(systemStore: SystemStore): Store

}