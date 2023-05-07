package org.chronos.chronostore.lsm.cache

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.lsm.LocalBlockCache
import org.chronos.chronostore.util.StoreId

class BlockCacheManagerImpl(
    config: ChronoStoreConfiguration
) : BlockCacheManager {

    private val cache: Cache<CacheKey, DataBlock> = CacheBuilder.newBuilder()
        .maximumWeight(config.blockCacheSizeInBytes)
        .weigher(this::computeBlockCacheWeight)
        .build()

    private fun computeBlockCacheWeight(key: CacheKey, value: DataBlock): Int {
        return value.metaData.byteSize
    }

    override fun getBlockCache(storeId: StoreId): LocalBlockCache {
        return LocalBlockCacheImpl(storeId)
    }

    private inner class LocalBlockCacheImpl(val storeId: StoreId) : LocalBlockCache {
        override fun getBlock(fileIndex: Int, blockOffset: Long, loader: (Long) -> DataBlock?): DataBlock? {
            val cacheKey = CacheKey(storeId, fileIndex, blockOffset)
            return this@BlockCacheManagerImpl.cache.get(cacheKey) {
                loader(blockOffset)
            }
        }

    }

    private data class CacheKey(
        val storeId: StoreId,
        val fileIndex: Int,
        val blockOffset: Long
    )

}