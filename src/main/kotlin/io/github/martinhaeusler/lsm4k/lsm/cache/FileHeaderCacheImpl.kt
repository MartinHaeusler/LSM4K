package io.github.martinhaeusler.lsm4k.lsm.cache

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import io.github.martinhaeusler.lsm4k.io.format.FileHeader
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFile
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsReporter
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize

class FileHeaderCacheImpl(
    val maxSize: BinarySize,
    val statisticsReporter: StatisticsReporter,
) : FileHeaderCache {

    private val cache: Cache<VirtualFile, FileHeader> = CacheBuilder.newBuilder()
        .maximumWeight(maxSize.bytes)
        .weigher(this::computeHeaderWeight)
        .removalListener<VirtualFile, FileHeader> { this.statisticsReporter.reportFileHeaderCacheEviction() }
        .build()

    private fun computeHeaderWeight(
        // The cache key is a necessary parameter to fulfill the guava cache API.
        // If we remove this parameter here, we couldn't use this method as
        // a method reference for the cache loader.
        @Suppress("UNUSED_PARAMETER") key: VirtualFile,
        value: FileHeader,
    ): Int {
        // The interface of the cache weigher demands the result to be an INT.
        // To prevent integer overflows, we limit the LONG we get back from the
        // size calculation into the INT range (unlikely to be exceeded) and then
        // convert it to an INT. The consequence is that we treat all headers
        // which are larger than approx. 2GB as the same size. However, if we
        // would ever reach the situation where a single file header is
        // larger than 2GB, this conversion would be the least of our worries...
        return value.sizeBytes.coerceIn(0, Int.MAX_VALUE.toLong()).toInt()
    }

    override fun getFileHeader(file: VirtualFile, load: () -> FileHeader): FileHeader {
        this.statisticsReporter.reportFileHeaderCacheRequest()
        return this.cache.get(file) {
            this.statisticsReporter.reportFileHeaderCacheMiss()
            load()
        }
    }

}