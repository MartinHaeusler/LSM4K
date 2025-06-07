package io.github.martinhaeusler.lsm4k.lsm.cache

import io.github.martinhaeusler.lsm4k.io.format.FileHeader
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFile
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsReporter
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize

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