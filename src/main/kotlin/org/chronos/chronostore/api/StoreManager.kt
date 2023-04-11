package org.chronos.chronostore.api

import org.chronos.chronostore.util.StoreId

interface StoreManager {

    fun getStoreByName(name: String): Store {
        return getStoreByNameOrNull(name)
            ?: throw IllegalArgumentException("There is no store with name '${name}'!")
    }

    fun getStoreByNameOrNull(name: String): Store?

    fun getStoreById(storeId: StoreId): Store {
        return getStoreByIdOrNull(storeId)
            ?: throw IllegalArgumentException("There is no store with ID '${storeId}'!")
    }

    fun getStoreByIdOrNull(storeId: StoreId): Store?

    fun existsStoreByName(name: String): Boolean {
        return this.getStoreByNameOrNull(name) != null
    }

    fun existsStoreById(storeId: StoreId): Boolean {
        return this.getStoreByIdOrNull(storeId) != null
    }

    fun createNewStore(name: String, versioned: Boolean): Store

    fun renameStore(oldName: String, newName: String)

    fun renameStore(storeId: StoreId, newName: String)

    fun deleteStoreByName(name: String): Boolean

    fun deleteStoreById(storeId: StoreId): Boolean

    fun getAllStores(): List<Store>


}