package io.github.martinhaeusler.lsm4k.lsm.cache

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import io.github.martinhaeusler.lsm4k.io.format.BlockLoader
import io.github.martinhaeusler.lsm4k.io.format.datablock.DataBlock
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFile
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsReporter
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize
import java.util.concurrent.CompletableFuture

class BlockCacheImpl(
    val maxSize: BinarySize,
    val loader: BlockLoader,
    val statisticsReporter: StatisticsReporter,
) : BlockCache {

    // TODO [DEPENDENCIES] Use Caffeine cache here, it supports async loading out of the box
    private val cache: Cache<CacheKey, DataBlock> = CacheBuilder.newBuilder()
        .maximumWeight(maxSize.bytes)
        .weigher(this::computeBlockCacheWeight)
        .removalListener<CacheKey, DataBlock> { this.statisticsReporter.reportBlockCacheEviction() }
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
        this.statisticsReporter.reportBlockCacheRequest()
        val cacheKey = CacheKey(file.path, blockIndex)
        val cachedResult = this.cache.getIfPresent(cacheKey)
        if (cachedResult != null) {
            return CompletableFuture.completedFuture(cachedResult)
        }
        this.statisticsReporter.reportBlockCacheMiss()
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