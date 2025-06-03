package org.chronos.chronostore.test.util.lsm

import org.chronos.chronostore.io.fileaccess.FileChannelDriver
import org.chronos.chronostore.io.fileaccess.InMemoryFileDriver
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.BlockLoader
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.io.format.writer.StandardChronoStoreFileWriter
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.io.vfs.inmemory.InMemoryVirtualFile
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.lsm.LSMTreeFile
import org.chronos.chronostore.lsm.cache.BlockCache
import org.chronos.chronostore.lsm.cache.FileHeaderCache
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.iterator.IteratorExtensions.toMutableList
import org.chronos.chronostore.util.statistics.StatisticsReporter
import org.chronos.chronostore.util.unit.BinarySize.Companion.MiB

object LsmFileFactory {

    fun VirtualFileSystem.createLsmTreeFile(
        index: FileIndex,
        content: Iterable<Command>,
        fileSettings: ChronoStoreFileSettings = ChronoStoreFileSettings(
            compression = CompressionAlgorithm.forCompressorName("snappy"),
            maxBlockSize = 8.MiB
        ),
        numberOfMerges: Long = 0,
        maxCompletelyWrittenTSN: TSN? = null,
        driverFactory: RandomFileAccessDriverFactory = FileChannelDriver.Factory,
        statisticsReporter: StatisticsReporter,
        fileHeaderCache: FileHeaderCache = FileHeaderCache.none(statisticsReporter),
        blockLoader: BlockLoader = BlockLoader.basic(driverFactory, statisticsReporter, fileHeaderCache),
        blockCache: BlockCache = BlockCache.none(blockLoader),
    ): LSMTreeFile {
        val file = this.file(LSMTree.createFileNameForIndex(index))
        return createLsmTreeFileFromVirtualFile(
            file = file,
            fileSettings = fileSettings,
            content = content.iterator(),
            numberOfMerges = numberOfMerges,
            maxCompletelyWrittenTSN = maxCompletelyWrittenTSN,
            index = index,
            driverFactory = driverFactory,
            blockLoader = blockLoader,
            blockCache = blockCache,
            fileHeaderCache = fileHeaderCache,
            statisticsReporter = statisticsReporter,
        )
    }

    fun VirtualFileSystem.createLsmTreeFile(
        index: FileIndex,
        content: Iterator<Command>,
        fileSettings: ChronoStoreFileSettings = ChronoStoreFileSettings(
            compression = CompressionAlgorithm.forCompressorName("snappy"),
            maxBlockSize = 8.MiB
        ),
        numberOfMerges: Long = 0,
        maxCompletelyWrittenTSN: TSN? = null,
        driverFactory: RandomFileAccessDriverFactory = FileChannelDriver.Factory,
        statisticsReporter: StatisticsReporter,
        fileHeaderCache: FileHeaderCache = FileHeaderCache.none(statisticsReporter),
        blockLoader: BlockLoader = BlockLoader.basic(driverFactory, statisticsReporter, fileHeaderCache),
        blockCache: BlockCache = BlockCache.none(blockLoader),
    ): LSMTreeFile {
        val file = this.file(LSMTree.createFileNameForIndex(index))
        return createLsmTreeFileFromVirtualFile(
            file = file,
            fileSettings = fileSettings,
            content = content,
            numberOfMerges = numberOfMerges,
            maxCompletelyWrittenTSN = maxCompletelyWrittenTSN,
            index = index,
            driverFactory = driverFactory,
            blockLoader = blockLoader,
            blockCache = blockCache,
            fileHeaderCache = fileHeaderCache,
            statisticsReporter = statisticsReporter,
        )
    }

    fun VirtualDirectory.createLsmTreeFile(
        index: FileIndex,
        content: Iterable<Command>,
        fileSettings: ChronoStoreFileSettings = ChronoStoreFileSettings(
            compression = CompressionAlgorithm.forCompressorName("snappy"),
            maxBlockSize = 8.MiB
        ),
        numberOfMerges: Long = 0,
        maxCompletelyWrittenTSN: TSN? = null,
        driverFactory: RandomFileAccessDriverFactory = FileChannelDriver.Factory,
        statisticsReporter: StatisticsReporter,
        fileHeaderCache: FileHeaderCache = FileHeaderCache.none(statisticsReporter),
        blockLoader: BlockLoader = BlockLoader.basic(driverFactory, statisticsReporter, fileHeaderCache),
        blockCache: BlockCache = BlockCache.none(blockLoader),
    ): LSMTreeFile {
        return this.createLsmTreeFile(
            index = index,
            content = content.iterator(),
            fileSettings = fileSettings,
            numberOfMerges = numberOfMerges,
            maxCompletelyWrittenTSN = maxCompletelyWrittenTSN,
            driverFactory = driverFactory,
            blockLoader = blockLoader,
            blockCache = blockCache,
            fileHeaderCache = fileHeaderCache,
            statisticsReporter = statisticsReporter,
        )
    }

    fun VirtualDirectory.createLsmTreeFile(
        index: FileIndex,
        content: Iterator<Command>,
        fileSettings: ChronoStoreFileSettings = ChronoStoreFileSettings(
            compression = CompressionAlgorithm.forCompressorName("snappy"),
            maxBlockSize = 8.MiB
        ),
        numberOfMerges: Long = 0,
        maxCompletelyWrittenTSN: TSN? = null,
        driverFactory: RandomFileAccessDriverFactory = FileChannelDriver.Factory,
        statisticsReporter: StatisticsReporter,
        fileHeaderCache: FileHeaderCache = FileHeaderCache.none(statisticsReporter),
        blockLoader: BlockLoader = BlockLoader.basic(driverFactory, statisticsReporter, fileHeaderCache),
        blockCache: BlockCache = BlockCache.none(blockLoader),
    ): LSMTreeFile {
        val file = this.file(LSMTree.createFileNameForIndex(index))
        return createLsmTreeFileFromVirtualFile(
            file = file,
            fileSettings = fileSettings,
            content = content,
            numberOfMerges = numberOfMerges,
            maxCompletelyWrittenTSN = maxCompletelyWrittenTSN,
            index = index,
            driverFactory = driverFactory,
            blockLoader = blockLoader,
            blockCache = blockCache,
            fileHeaderCache = fileHeaderCache,
            statisticsReporter = statisticsReporter,
        )
    }

    private fun createLsmTreeFileFromVirtualFile(
        file: VirtualReadWriteFile,
        fileSettings: ChronoStoreFileSettings,
        content: Iterator<Command>,
        numberOfMerges: Long,
        maxCompletelyWrittenTSN: TSN?,
        index: FileIndex,
        driverFactory: RandomFileAccessDriverFactory = FileChannelDriver.Factory,
        statisticsReporter: StatisticsReporter,
        fileHeaderCache: FileHeaderCache = FileHeaderCache.none(statisticsReporter),
        blockLoader: BlockLoader = BlockLoader.basic(driverFactory, statisticsReporter, fileHeaderCache),
        blockCache: BlockCache = BlockCache.none(blockLoader),
    ): LSMTreeFile {
        val driverFactory = when (file) {
            is InMemoryVirtualFile -> InMemoryFileDriver.Factory
            else -> FileChannelDriver.Factory
        }

        // write the contents
        file.deleteIfExists()
        file.deleteOverWriterFileIfExists()
        file.createOverWriter().use { overWriter ->
            StandardChronoStoreFileWriter(overWriter.outputStream, fileSettings, statisticsReporter).use { fileWriter ->
                // for unit tests, we expect small data sets, so it's ok to sort them explicitly here
                val contentList = content.toMutableList()
                contentList.sorted()
                fileWriter.write(
                    numberOfMerges = numberOfMerges,
                    orderedCommands = contentList.iterator(),
                    commandCountEstimate = contentList.size.toLong(),
                    maxCompletelyWrittenTSN = maxCompletelyWrittenTSN,
                )
            }
            overWriter.commit()
        }

        return LSMTreeFile(
            virtualFile = file,
            index = index,
            driverFactory = driverFactory,
            blockCache = blockCache,
            fileHeaderCache = fileHeaderCache,
        )
    }

}