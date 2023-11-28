package org.chronos.chronostore.lsm.cache

import org.chronos.chronostore.io.format.FileHeader
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics

data object NoFileHeaderCache: FileHeaderCache {

    override fun getFileHeader(file: VirtualFile, load: () -> FileHeader): FileHeader {
        // we don't have a cache, so the only choice is to load the file
        // header every time it gets accessed.
        ChronoStoreStatistics.FILE_HEADER_CACHE_REQUESTS.incrementAndGet()
        ChronoStoreStatistics.FILE_HEADER_CACHE_MISSES.incrementAndGet()
        return load()
    }

}