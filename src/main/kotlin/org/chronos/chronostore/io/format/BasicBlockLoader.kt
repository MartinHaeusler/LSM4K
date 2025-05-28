package org.chronos.chronostore.io.format

import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.lsm.cache.FileHeaderCache
import org.chronos.chronostore.util.statistics.StatisticsReporter
import java.util.concurrent.CompletableFuture

class BasicBlockLoader(
    private val driverFactory: RandomFileAccessDriverFactory,
    private val headerCache: FileHeaderCache,
    private val statisticsReporter: StatisticsReporter,
) : BlockLoader {

    override val isAsyncSupported: Boolean
        get() = false

    override fun getBlockAsync(file: VirtualFile, blockIndex: Int): CompletableFuture<DataBlock?> {
        this.driverFactory.createDriver(file).use { driver ->
            val header = this.headerCache.getFileHeader(file) { ChronoStoreFileFormat.loadFileHeader(driver) }
            val block = ChronoStoreFileFormat.loadBlockFromFileOrNull(
                driver = driver,
                fileHeader = header,
                blockIndex = blockIndex,
                statisticsReporter = this.statisticsReporter,
            )
            return CompletableFuture.completedFuture(block)
        }
    }

}