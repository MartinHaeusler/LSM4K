package org.chronos.chronostore.lsm.cache

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.util.StoreId
import java.util.UUID

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


        override fun getBlock(fileId: UUID, blockIndex: Int, loader: (Int) -> DataBlock?): DataBlock? {
            val cacheKey = CacheKey(storeId, fileId, blockIndex)
            return this@BlockCacheManagerImpl.cache.get(cacheKey) {
                loader(blockIndex)
            }
        }

    }

    private data class CacheKey(
        val storeId: StoreId,
        val fileId: UUID,
        val blockIndex: Int
    )

}