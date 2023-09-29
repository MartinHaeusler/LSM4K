package org.chronos.chronostore.lsm.cache

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.unit.BinarySize
import java.util.*

class BlockCacheManagerImpl(
    val maxSize: BinarySize
) : BlockCacheManager {

    private val cache: Cache<CacheKey, DataBlock> = CacheBuilder.newBuilder()
        .maximumWeight(maxSize.bytes)
        .weigher(this::computeBlockCacheWeight)
        .build()

    private fun computeBlockCacheWeight(
        // The cache key is a necessary parameter to fulfill the guava cache API.
        // If we would remove this parameter here, we couldn't use this method as
        // a method reference for the cache loader.
        @Suppress("UNUSED_PARAMETER") key: CacheKey,
        value: DataBlock
    ): Int {
        return value.metaData.byteSize
    }

    override fun getBlockCache(storeId: StoreId): LocalBlockCache {
        return LocalBlockCacheImpl(storeId)
    }

    private inner class LocalBlockCacheImpl(
        override val storeId: StoreId
    ) : LocalBlockCache {


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