package org.chronos.chronostore.lsm.cache

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import org.chronos.chronostore.io.format.BlockLoader
import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import org.chronos.chronostore.util.unit.BinarySize
import java.util.concurrent.CompletableFuture

class BlockCacheImpl(
    val maxSize: BinarySize,
    val loader: BlockLoader,
) : BlockCache {

    // TODO [DEPENDENCIES] Use Caffeine cache here, it supports async loading out of the box
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

    override fun getBlockAsync(file: VirtualFile, blockIndex: Int): CompletableFuture<DataBlock?> {
        val cacheKey = CacheKey(file.path, blockIndex)
        val cachedResult = this.cache.getIfPresent(cacheKey)
        if (cachedResult != null) {
            return CompletableFuture.completedFuture(cachedResult)
        }
        return this.loader.getBlockAsync(file, blockIndex)
            .thenApply { dataBlock ->
                if (dataBlock != null) {
                    this.cache.put(cacheKey, dataBlock)
                }
                dataBlock
            }
    }

    override val isAsyncSupported: Boolean
        get() = this.loader.isAsyncSupported


    private data class CacheKey(
        val filePath: String,
        val blockIndex: Int,
    )

}