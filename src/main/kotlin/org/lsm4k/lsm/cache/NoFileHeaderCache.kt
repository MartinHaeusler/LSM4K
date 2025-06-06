package org.lsm4k.lsm.cache

import org.lsm4k.io.format.FileHeader
import org.lsm4k.io.vfs.VirtualFile
import org.lsm4k.util.statistics.StatisticsReporter

class NoFileHeaderCache(
    val statisticsReporter: StatisticsReporter,
) : FileHeaderCache {

    override fun getFileHeader(file: VirtualFile, load: () -> FileHeader): FileHeader {
        // we don't have a cache, so the only choice is to load the file
        // header every time it gets accessed.
        this.statisticsReporter.reportFileHeaderCacheRequest()
        this.statisticsReporter.reportFileHeaderCacheMiss()
        return load()
    }

}