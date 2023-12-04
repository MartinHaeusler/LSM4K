package org.chronos.chronostore.benchmark.comparative.chronostore

import org.chronos.chronostore.api.exceptions.ChronoStoreBlockReadException
import org.chronos.chronostore.benchmark.util.NumberUtils.format
import org.chronos.chronostore.io.fileaccess.FileChannelDriver
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory.Companion.withDriver
import org.chronos.chronostore.io.format.ChronoStoreFileReader
import org.chronos.chronostore.io.format.ChronoStoreFileReader.Companion.withChronoStoreFileReader
import org.chronos.chronostore.io.format.FileHeader
import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystem
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystemSettings
import org.chronos.chronostore.lsm.cache.LocalBlockCache
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.util.unit.Bytes
import java.io.File
import java.util.*

object StoreFileDebug {

    @JvmStatic
    fun main(args: Array<String>) {
        // val inputDir = File("/home/martin/Documents/chronostore-test/corruptedData")
        val inputDir = File("/home/martin/Documents/chronostore-test/taxonomyChronoStore/data")
        val chronoStoreFileNames = inputDir.listFiles().asSequence()
            .filter { it.name.endsWith(".chronostore") }
            .map { it.name }
            .sorted()
            .toList()
        val vfs = DiskBasedVirtualFileSystem(inputDir, DiskBasedVirtualFileSystemSettings())
        var overallEntries = 0
        for (chronoStoreFileName in chronoStoreFileNames) {
            val file = vfs.file(chronoStoreFileName)
            val localEntries = countEntriesInFile(file)
            println("File '${file.name}' has ${localEntries} entries (counted).")
            overallEntries += localEntries
            val header = loadHeader(file)
            println("File '${file.name}' header:")
            println("  - Entries: ${header.metaData.totalEntries}")
            println("    - Head: ${header.metaData.headEntries}")
            println("    - Hist: ${header.metaData.historyEntries}")
            println("    - HHR: ${header.metaData.headHistoryRatio.format("%.3f")}")
            println("  - Blocks: ${header.metaData.numberOfBlocks}")
            println("  - Max. Block Size: ${header.metaData.settings.maxBlockSize.toHumanReadableString()}")
            println("  - Compression: ${header.metaData.settings.compression}")
            println("  - Created at: ${Date(header.metaData.createdAt)}")
            println("  - Merges: ${header.metaData.numberOfMerges}")
            println("  - File Format Version: ${header.fileFormatVersion}")
            println("File '${file.name}' blocks:")
            for (blockIndex in 0..<header.metaData.numberOfBlocks) {
                FileChannelDriver.Factory.withDriver(file) { driver ->
                    val dataBlock = readBlock(driver, header, blockIndex)
                    println("  - Block #${blockIndex}")
                    println("    - Size (compressed): ${dataBlock.metaData.compressedDataSize.Bytes.toHumanReadableString()}")
                    println("    - Size (uncompressed): ${dataBlock.metaData.uncompressedDataSize.Bytes.toHumanReadableString()}")
                    println("    - Min Timestamp: ${dataBlock.metaData.minTimestamp}")
                    println("    - Max Timestamp: ${dataBlock.metaData.maxTimestamp}")
                    println("    - Min Key: ${dataBlock.metaData.minKey.asString()}")
                    println("    - Max Key: ${dataBlock.metaData.maxKey.asString()}")
                    println("    - Entries (header): ${dataBlock.metaData.commandCount}")
                    println("    - Entries (count): ${dataBlock.withCursor { it.firstOrThrow(); it.ascendingKeySequenceFromHere().count() }}")

                }
            }

        }
        println("=========================================================")
        println("Sum: ${overallEntries} entries in *.chronostore files")
    }

    private fun countEntriesInFile(file: VirtualReadWriteFile): Int {
        FileChannelDriver.Factory.withDriver(file) { driver ->
            val entries = driver.withChronoStoreFileReader(LocalBlockCache.NONE) { reader ->
                reader.withCursor { cursor ->
                    cursor.firstOrThrow()
                    cursor.ascendingKeySequenceFromHere().count()
                }
            }
            return entries
        }
    }

    private fun loadHeader(file: VirtualReadWriteFile): FileHeader {
        FileChannelDriver.Factory.withDriver(file) { driver ->
            return ChronoStoreFileReader.loadFileHeader(driver)
        }
    }

    private inline fun <T> VirtualReadWriteFile.withReader(action: (ChronoStoreFileReader) -> T): T {
        FileChannelDriver.Factory.withDriver(this) { driver ->
            return driver.withChronoStoreFileReader(LocalBlockCache.NONE, action)
        }
    }

    private fun readBlock(
        driver: RandomFileAccessDriver,
        fileHeader: FileHeader,
        blockIndex: Int,
    ): DataBlock {
        val (startPosition, length) = fileHeader.indexOfBlocks.getBlockStartPositionAndLengthOrNull(blockIndex)
            ?: throw IllegalStateException("Block Index ${blockIndex} is in range but no block was found for it!")
        val blockBytes = driver.readBytes(startPosition, length)
        val compressionAlgorithm = fileHeader.metaData.settings.compression
        try {
            return DataBlock.loadBlock(blockBytes, compressionAlgorithm)
        } catch (e: Exception) {
            throw ChronoStoreBlockReadException(
                message = "Failed to read block #${blockIndex} of file '${driver.filePath}'." +
                    " This file is potentially corrupted! Cause: ${e}",
                cause = e
            )
        }
    }

}