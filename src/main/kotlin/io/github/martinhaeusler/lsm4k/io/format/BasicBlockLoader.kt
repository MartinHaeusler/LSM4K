package io.github.martinhaeusler.lsm4k.io.format

import io.github.martinhaeusler.lsm4k.io.fileaccess.RandomFileAccessDriverFactory
import io.github.martinhaeusler.lsm4k.io.format.datablock.DataBlock
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFile
import io.github.martinhaeusler.lsm4k.lsm.cache.FileHeaderCache
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsReporter
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
            val header = this.headerCache.getFileHeader(file) { LSMFileFormat.loadFileHeader(driver) }
            val block = LSMFileFormat.loadBlockFromFileOrNull(
                driver = driver,
                fileHeader = header,
                blockIndex = blockIndex,
                statisticsReporter = this.statisticsReporter,
            )
            return CompletableFuture.completedFuture(block)
        }
    }

}