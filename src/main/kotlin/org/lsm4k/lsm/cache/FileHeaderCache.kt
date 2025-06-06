package org.lsm4k.lsm.cache

import org.lsm4k.io.format.FileHeader
import org.lsm4k.io.vfs.VirtualFile
import org.lsm4k.util.statistics.StatisticsReporter
import org.lsm4k.util.unit.BinarySize

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