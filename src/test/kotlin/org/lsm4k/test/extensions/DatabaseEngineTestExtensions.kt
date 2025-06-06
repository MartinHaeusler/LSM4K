package org.lsm4k.test.extensions

import org.lsm4k.api.DatabaseEngine
import org.lsm4k.api.Store
import org.lsm4k.impl.DatabaseEngineImpl
import org.lsm4k.util.StoreId
import java.util.concurrent.CompletableFuture

object DatabaseEngineTestExtensions {

    fun DatabaseEngine.flushStoreSynchronous(storeId: String) {
        (this as DatabaseEngineImpl).flushSynchronous(storeId)
    }

    fun DatabaseEngine.flushStoreSynchronous(storeId: StoreId) {
        (this as DatabaseEngineImpl).flushSynchronous(storeId)
    }

    fun DatabaseEngine.flushStoreSynchronous(store: Store) {
        (this as DatabaseEngineImpl).flushSynchronous(store.storeId)
    }

    fun DatabaseEngine.flushAllStoresSynchronous() {
        (this as DatabaseEngineImpl).flushAllStoresSynchronous()
    }

    fun DatabaseEngine.majorCompactionOnAllStoresAsync(): CompletableFuture<*> {
        return (this as DatabaseEngineImpl).majorCompactionOnAllStoresAsync()
    }

    fun DatabaseEngine.majorCompactionOnAllStoresSynchronous() {
        return (this as DatabaseEngineImpl).majorCompactionOnAllStoresSynchronous()
    }

    fun DatabaseEngine.majorCompactionOnStoreAsync(storeId: StoreId): CompletableFuture<*> {
        return (this as DatabaseEngineImpl).majorCompactionOnStoreAsync(storeId)
    }

    fun DatabaseEngine.majorCompactionOnStoreAsync(storeId: String): CompletableFuture<*> {
        return (this as DatabaseEngineImpl).majorCompactionOnStoreAsync(storeId)
    }

    fun DatabaseEngine.majorCompactionOnStoreSynchronous(storeId: StoreId) {
        (this as DatabaseEngineImpl).majorCompactionOnStoreSynchronous(storeId)
    }

    fun DatabaseEngine.majorCompactionOnStoreSynchronous(storeId: String) {
        (this as DatabaseEngineImpl).majorCompactionOnStoreSynchronous(storeId)
    }

    fun DatabaseEngine.minorCompactionOnAllStoresAsync(): CompletableFuture<*> {
        return (this as DatabaseEngineImpl).minorCompactionOnAllStoresAsync()
    }

    fun DatabaseEngine.minorCompactionOnAllStoresSynchronous() {
        (this as DatabaseEngineImpl).minorCompactionOnAllStoresSynchronous()
    }

    fun DatabaseEngine.minorCompactionOnStoreAsync(storeId: StoreId): CompletableFuture<*> {
        return (this as DatabaseEngineImpl).minorCompactionOnStoreAsync(storeId)
    }

    fun DatabaseEngine.minorCompactionOnStoreAsync(storeId: String): CompletableFuture<*> {
        return (this as DatabaseEngineImpl).minorCompactionOnStoreAsync(storeId)
    }

    fun DatabaseEngine.minorCompactionOnStoreSynchronous(storeId: StoreId) {
        return (this as DatabaseEngineImpl).minorCompactionOnStoreSynchronous(storeId)
    }

    fun DatabaseEngine.minorCompactionOnStoreSynchronous(storeId: String) {
        return (this as DatabaseEngineImpl).minorCompactionOnStoreSynchronous(storeId)
    }

}