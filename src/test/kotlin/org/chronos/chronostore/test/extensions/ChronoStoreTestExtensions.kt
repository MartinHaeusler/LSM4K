package org.chronos.chronostore.test.extensions

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.impl.ChronoStoreImpl
import org.chronos.chronostore.lsm.compaction.strategy.MergeService
import org.chronos.chronostore.util.StoreId

object ChronoStoreTestExtensions {

    fun ChronoStore.flush(storeId: String){
        val mergeService: MergeService = (this as ChronoStoreImpl).mergeService
        mergeService.flushInMemoryStoreToDisk(storeId)
    }

    fun ChronoStore.flush(storeId: StoreId){
        val mergeService: MergeService = (this as ChronoStoreImpl).mergeService
        mergeService.flushInMemoryStoreToDisk(storeId)
    }

    fun ChronoStore.flush(store: Store){
        val mergeService: MergeService = (this as ChronoStoreImpl).mergeService
        mergeService.flushInMemoryStoreToDisk(store.storeId)
    }

    fun ChronoStore.flushAll(){
        val mergeService: MergeService = (this as ChronoStoreImpl).mergeService
        mergeService.flushAllInMemoryStoresToDisk()
    }

}