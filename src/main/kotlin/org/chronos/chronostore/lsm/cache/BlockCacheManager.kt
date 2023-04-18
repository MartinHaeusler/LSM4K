package org.chronos.chronostore.lsm.cache

import org.chronos.chronostore.lsm.LocalBlockCache
import org.chronos.chronostore.util.StoreId

interface BlockCacheManager {

    fun getBlockCache(storeId: StoreId): LocalBlockCache


}