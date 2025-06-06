package org.lsm4k.io.format

import org.lsm4k.api.exceptions.BlockReadException
import org.lsm4k.io.fileaccess.RandomFileAccessDriverFactory
import org.lsm4k.io.format.datablock.DataBlock
import org.lsm4k.io.vfs.VirtualFile
import org.lsm4k.lsm.cache.FileHeaderCache
import org.lsm4k.util.statistics.StatisticsReporter
import java.util.concurrent.CompletableFuture

interface BlockLoader {

    companion object {

        fun basic(
            driverFactory: RandomFileAccessDriverFactory,
            statisticsReporter: StatisticsReporter,
            headerCache: FileHeaderCache = FileHeaderCache.none(statisticsReporter),
        ): BlockLoader {
            return BasicBlockLoader(
                driverFactory = driverFactory,
                headerCache = headerCache,
                statisticsReporter = statisticsReporter
            )
        }

    }

    val isAsyncSupported: Boolean

    fun getBlockOrNull(file: VirtualFile, blockIndex: Int): DataBlock? {
        return this.getBlockAsync(file, blockIndex).get()
    }

    fun getBlock(file: VirtualFile, blockIndex: Int): DataBlock {
        return this.getBlockOrNull(file, blockIndex)
            ?: throw BlockReadException("Could not read block #${blockIndex} from file '${file.path}' because the file does not have that many blocks!")
    }

    fun getBlockAsync(file: VirtualFile, blockIndex: Int): CompletableFuture<DataBlock?>

}