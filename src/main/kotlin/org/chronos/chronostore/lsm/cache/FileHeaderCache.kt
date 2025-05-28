package org.chronos.chronostore.lsm.cache

import org.chronos.chronostore.io.format.FileHeader
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.util.statistics.StatisticsReporter
import org.chronos.chronostore.util.unit.BinarySize

sealed interface FileHeaderCache {

    fun getFileHeader(file: VirtualFile, load: () -> FileHeader): FileHeader

    companion object {

        fun none(statisticsReporter: StatisticsReporter): FileHeaderCache {
            return NoFileHeaderCache(statisticsReporter)
        }

        fun create(fileHeaderCacheSize: BinarySize?, statisticsReporter: StatisticsReporter): FileHeaderCache {
            return if (fileHeaderCacheSize != null) {
                FileHeaderCacheImpl(
                    maxSize = fileHeaderCacheSize,
                    statisticsReporter = statisticsReporter
                )
            } else {
                NoFileHeaderCache(statisticsReporter)
            }
        }

    }

}