package io.github.martinhaeusler.lsm4k.api

import io.github.martinhaeusler.lsm4k.api.compaction.CompactionStrategy
import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.TSN

interface StoreManager {

    fun getStore(transaction: LSM4KTransaction, name: StoreId): Store {
        return getStoreByIdOrNull(transaction, name)
            ?: throw IllegalArgumentException("There is no store with name '${name}'!")
    }

    fun getStoreByIdOrNull(transaction: LSM4KTransaction, name: StoreId): Store?

    fun existsStore(transaction: LSM4KTransaction, name: StoreId): Boolean {
        return this.getStoreByIdOrNull(transaction, name) != null
    }

    fun getHighWatermarkTSN(): TSN?

    fun getLowWatermarkTSN(): TSN?

    fun createNewStore(transaction: LSM4KTransaction, storeId: StoreId, validFromTSN: TSN, compactionStrategy: CompactionStrategy?): Store

    fun deleteStore(transaction: LSM4KTransaction, storeId: StoreId): Boolean

    fun getAllStores(transaction: LSM4KTransaction): List<Store>

    fun <T> withStoreReadLock(action: () -> T): T

    fun performGarbageCollection(monitor: TaskMonitor)

    fun getSystemStore(systemStore: SystemStore): Store

}