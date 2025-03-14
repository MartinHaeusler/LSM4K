package org.chronos.chronostore.test.extensions

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.impl.ChronoStoreImpl
import org.chronos.chronostore.test.extensions.ChronoStoreTestExtensions.majorCompactionOnAllStoresSynchronous
import org.chronos.chronostore.test.extensions.ChronoStoreTestExtensions.majorCompactionOnStoreAsync
import org.chronos.chronostore.test.extensions.ChronoStoreTestExtensions.majorCompactionOnStoreSynchronous
import org.chronos.chronostore.util.StoreId
import java.util.concurrent.CompletableFuture

object ChronoStoreTestExtensions {

    fun ChronoStore.flushStoreSynchronous(storeId: String) {
        (this as ChronoStoreImpl).flushSynchronous(storeId)
    }

    fun ChronoStore.flushStoreSynchronous(storeId: StoreId) {
        (this as ChronoStoreImpl).flushSynchronous(storeId)
    }

    fun ChronoStore.flushStoreSynchronous(store: Store) {
        (this as ChronoStoreImpl).flushSynchronous(store.storeId)
    }

    fun ChronoStore.flushAllStoresSynchronous() {
        (this as ChronoStoreImpl).flushAllStoresSynchronous()
    }

    fun ChronoStore.majorCompactionOnAllStoresAsync(): CompletableFuture<*> {
        return (this as ChronoStoreImpl).majorCompactionOnAllStoresAsync()
    }

    fun ChronoStore.majorCompactionOnAllStoresSynchronous() {
        return (this as ChronoStoreImpl).majorCompactionOnAllStoresSynchronous()
    }

    fun ChronoStore.majorCompactionOnStoreAsync(storeId: StoreId): CompletableFuture<*> {
        return (this as ChronoStoreImpl).majorCompactionOnStoreAsync(storeId)
    }

    fun ChronoStore.majorCompactionOnStoreAsync(storeId: String): CompletableFuture<*> {
        return (this as ChronoStoreImpl).majorCompactionOnStoreAsync(storeId)
    }

    fun ChronoStore.majorCompactionOnStoreSynchronous(storeId: StoreId) {
        (this as ChronoStoreImpl).majorCompactionOnStoreSynchronous(storeId)
    }

    fun ChronoStore.majorCompactionOnStoreSynchronous(storeId: String) {
        (this as ChronoStoreImpl).majorCompactionOnStoreSynchronous(storeId)
    }

    fun ChronoStore.minorCompactionOnAllStoresAsync(): CompletableFuture<*> {
        return (this as ChronoStoreImpl).minorCompactionOnAllStoresAsync()
    }

    fun ChronoStore.minorCompactionOnAllStoresSynchronous() {
        (this as ChronoStoreImpl).minorCompactionOnAllStoresSynchronous()
    }

    fun ChronoStore.minorCompactionOnStoreAsync(storeId: StoreId): CompletableFuture<*> {
        return (this as ChronoStoreImpl).minorCompactionOnStoreAsync(storeId)
    }

    fun ChronoStore.minorCompactionOnStoreAsync(storeId: String): CompletableFuture<*> {
        return (this as ChronoStoreImpl).minorCompactionOnStoreAsync(storeId)
    }

    fun ChronoStore.minorCompactionOnStoreSynchronous(storeId: StoreId) {
        return (this as ChronoStoreImpl).minorCompactionOnStoreSynchronous(storeId)
    }

    fun ChronoStore.minorCompactionOnStoreSynchronous(storeId: String) {
        return (this as ChronoStoreImpl).minorCompactionOnStoreSynchronous(storeId)
    }
}