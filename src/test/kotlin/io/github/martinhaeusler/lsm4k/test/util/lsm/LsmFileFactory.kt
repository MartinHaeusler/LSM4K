package io.github.martinhaeusler.lsm4k.test.util.lsm

import io.github.martinhaeusler.lsm4k.io.fileaccess.FileChannelDriver
import io.github.martinhaeusler.lsm4k.io.fileaccess.InMemoryFileDriver
import io.github.martinhaeusler.lsm4k.io.fileaccess.RandomFileAccessDriverFactory
import io.github.martinhaeusler.lsm4k.io.format.BlockLoader
import io.github.martinhaeusler.lsm4k.io.format.CompressionAlgorithm
import io.github.martinhaeusler.lsm4k.io.format.LSMFileSettings
import io.github.martinhaeusler.lsm4k.io.format.writer.StandardLSMStoreFileWriter
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualDirectory
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFileSystem
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualReadWriteFile
import io.github.martinhaeusler.lsm4k.io.vfs.inmemory.InMemoryVirtualFile
import io.github.martinhaeusler.lsm4k.lsm.LSMTree
import io.github.martinhaeusler.lsm4k.lsm.LSMTreeFile
import io.github.martinhaeusler.lsm4k.lsm.cache.BlockCache
import io.github.martinhaeusler.lsm4k.lsm.cache.FileHeaderCache
import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.util.FileIndex
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.util.iterator.IteratorExtensions.toMutableList
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsReporter
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.MiB

object LsmFileFactory {

    fun VirtualFileSystem.createLsmTreeFile(
        index: FileIndex,
        content: Iterable<Command>,
        fileSettings: LSMFileSettings = LSMFileSettings(
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
        fileSettings: LSMFileSettings = LSMFileSettings(
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
        fileSettings: LSMFileSettings = LSMFileSettings(
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
        fileSettings: LSMFileSettings = LSMFileSettings(
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
        fileSettings: LSMFileSettings,
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
            StandardLSMStoreFileWriter(overWriter.outputStream, fileSettings, statisticsReporter).use { fileWriter ->
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