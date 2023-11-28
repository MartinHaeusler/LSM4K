package org.chronos.chronostore.lsm.cache

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import org.chronos.chronostore.util.unit.BinarySize
import java.util.*

class BlockCacheManagerImpl(
    val maxSize: BinarySize,
) : BlockCacheManager {

    private val cache: Cache<CacheKey, DataBlock> = CacheBuilder.newBuilder()
        .maximumWeight(maxSize.bytes)
        .weigher(this::computeBlockCacheWeight)
        .removalListener<CacheKey, DataBlock> { ChronoStoreStatistics.BLOCK_CACHE_EVICTIONS.incrementAndGet() }
        .build()

    private fun computeBlockCacheWeight(
        // The cache key is a necessary parameter to fulfill the guava cache API.
        // If we remove this parameter here, we couldn't use this method as
        // a method reference for the cache loader.
        @Suppress("UNUSED_PARAMETER") key: CacheKey,
        value: DataBlock,
    ): Int {
        val weight = value.byteSize
        return if (weight > Int.MAX_VALUE) {
            Int.MAX_VALUE
        } else {
            weight.toInt()
        }
    }

    override fun getBlockCache(storeId: StoreId): LocalBlockCache {
        return LocalBlockCacheImpl(storeId)
    }

    private inner class LocalBlockCacheImpl(
        override val storeId: StoreId,
    ) : LocalBlockCache {


        override fun getBlock(fileId: UUID, blockIndex: Int, loader: (Int) -> DataBlock?): DataBlock? {
            ChronoStoreStatistics.BLOCK_CACHE_REQUESTS.incrementAndGet()
            val cacheKey = CacheKey(storeId, fileId, blockIndex)
            return this@BlockCacheManagerImpl.cache.get(cacheKey) {
                ChronoStoreStatistics.BLOCK_CACHE_MISSES.incrementAndGet()
                loader(blockIndex)
            }
        }

    }

    private data class CacheKey(
        val storeId: StoreId,
        val fileId: UUID,
        val blockIndex: Int,
    )

}