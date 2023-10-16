package org.chronos.chronostore.lsm.cache

import org.chronos.chronostore.util.StoreId

data object NoBlockCacheManager: BlockCacheManager {

    override fun getBlockCache(storeId: StoreId): LocalBlockCache {
        return NoBlockCache
    }

}