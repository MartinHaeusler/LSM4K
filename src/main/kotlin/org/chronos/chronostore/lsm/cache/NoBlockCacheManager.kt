package org.chronos.chronostore.lsm.cache

import org.chronos.chronostore.util.StoreId

object NoBlockCacheManager: BlockCacheManager {

    override fun getBlockCache(storeId: StoreId): LocalBlockCache {
        return NoBlockCache
    }

}