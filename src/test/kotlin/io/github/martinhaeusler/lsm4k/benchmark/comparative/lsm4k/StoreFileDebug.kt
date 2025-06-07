package io.github.martinhaeusler.lsm4k.benchmark.comparative.lsm4k

import io.github.martinhaeusler.lsm4k.benchmark.util.NumberUtils.format
import io.github.martinhaeusler.lsm4k.io.fileaccess.FileChannelDriver
import io.github.martinhaeusler.lsm4k.io.fileaccess.RandomFileAccessDriverFactory.Companion.withDriver
import io.github.martinhaeusler.lsm4k.io.format.BlockLoader
import io.github.martinhaeusler.lsm4k.io.format.FileHeader
import io.github.martinhaeusler.lsm4k.io.format.LSMFileFormat
import io.github.martinhaeusler.lsm4k.io.format.cursor.LSMFileCursor
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualReadWriteFile
import io.github.martinhaeusler.lsm4k.io.vfs.disk.DiskBasedVirtualFileSystem
import io.github.martinhaeusler.lsm4k.io.vfs.disk.DiskBasedVirtualFileSystemSettings
import io.github.martinhaeusler.lsm4k.lsm.cache.FileHeaderCache
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsCollector
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsReporter
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.Bytes
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.MiB
import java.io.File
import java.util.*

object StoreFileDebug {

    @JvmStatic
    fun main(args: Array<String>) {
        val inputDir = File("/home/martin/Documents/lsm4k-test/corruptedData")
        // val inputDir = File("/home/martin/Documents/lsm4k-test/taxonomyLSM4k/data")
        val storeFileNames = inputDir.listFiles().orEmpty().asSequence()
            .filter { it.name.endsWith(".lsm") }
            .map { it.name }
            .sorted()
            .toList()
        val vfs = DiskBasedVirtualFileSystem(inputDir, DiskBasedVirtualFileSystemSettings())
        var overallEntries = 0L
        val issues = mutableListOf<Issue>()

        val stats = StatisticsCollector()

        for (storeFileName in storeFileNames) {
            val file = vfs.file(storeFileName)
            val entriesInFile = countEntriesInFile(file, stats)
            println()
            println("File '${file.name}' has ${entriesInFile} entries (counted).")
            overallEntries += entriesInFile
            val header = loadHeader(file)
            println("File '${file.name}' header:")
            println("  - File UUID: ${header.metaData.fileUUID}")
            println("  - Header Size: ${header.sizeBytes.Bytes.toHumanReadableString()}")
            println("  - Entries: ${header.metaData.totalEntries}")
            if (entriesInFile != header.metaData.totalEntries) {
                val msg = "[ERROR] The header count (${header.metaData.totalEntries}) doesn't match the actual number of entries in the file ${entriesInFile}!"
                issues += Issue(fileName = storeFileName, blockIndex = null, message = msg)
                println(msg)
            }
            println("    - Head: ${header.metaData.headEntries}")
            println("    - Hist: ${header.metaData.historyEntries}")
            println("    - HHR: ${header.metaData.headHistoryRatio.format("%.3f")}")
            println("  - Blocks: ${header.metaData.numberOfBlocks}")
            println("  - Max. Block Size: ${header.metaData.settings.maxBlockSize.toHumanReadableString()}")
            println("  - Compression: ${header.metaData.settings.compression}")
            println("  - Created at: ${Date(header.metaData.createdAt)}")
            println("  - Merges: ${header.metaData.numberOfMerges}")
            println("  - File Format Version: ${header.fileFormatVersion}")

            if (header.indexOfBlocks.size != header.metaData.numberOfBlocks) {
                val msg =
                    "[ERROR] The number of blocks specified in the header (${header.metaData.numberOfBlocks}) doesn't match the actual number of blocks in the index ${header.indexOfBlocks.size}!"
                issues += Issue(fileName = storeFileName, blockIndex = null, message = msg)
                println(msg)
            }

            println("File '${file.name}' blocks:")
            for (blockIndex in 0..<header.metaData.numberOfBlocks) {
                FileChannelDriver.Factory.withDriver(file) { driver ->
                    val dataBlock = LSMFileFormat.loadBlockFromFile(driver, header, blockIndex, stats)
                    println("  - Block #${blockIndex}")
                    println("    - Size (compressed): ${dataBlock.metaData.compressedDataSize.Bytes.toHumanReadableString()}")
                    println("    - Size (uncompressed): ${dataBlock.metaData.uncompressedDataSize.Bytes.toHumanReadableString()}")
                    println("    - Min TSN: ${dataBlock.metaData.minTSN}")
                    println("    - Max TSN: ${dataBlock.metaData.maxTSN}")
                    println("    - Min Key: ${dataBlock.metaData.minKey.asString()}")
                    println("    - Max Key: ${dataBlock.metaData.maxKey.asString()}")
                    println("    - Entries (header): ${dataBlock.metaData.commandCount}")
                    val blockEntriesCount = dataBlock.withCursor { it.firstOrThrow(); it.ascendingKeySequenceFromHere().count() }
                    println("    - Entries (count): $blockEntriesCount")

                    if (blockEntriesCount != dataBlock.metaData.commandCount) {
                        val msg = "[ERROR] The header count (${header.metaData.totalEntries}) doesn't match the actual number of entries in block #${blockIndex} in file ${entriesInFile}!"
                        issues += Issue(fileName = storeFileName, blockIndex = blockIndex, message = msg)
                        println(msg)
                    }
                }
            }

        }
        println("=========================================================")
        println("Sum: ${overallEntries} entries in *.lsm files")
        println()
        println()
        if (issues.isNotEmpty()) {
            println("ISSUES")
            println("================================")
            for ((fileName, fileIssues) in issues.groupBy { it.fileName }) {
                println()
                println("File: ${fileName} (${fileIssues.size} issues)")
                println("-------------------------------------------------------")
                println()
                for (fileIssue in fileIssues) {
                    println(fileIssue.message)
                }
            }
        } else {
            println("No issues found in file integrity.")
        }
    }

    private fun countEntriesInFile(file: VirtualReadWriteFile, statisticsReporter: StatisticsReporter): Long {
        val driverFactory = FileChannelDriver.Factory
        driverFactory.createDriver(file).use { driver ->
            val fileHeaderCache = FileHeaderCache.create(100.MiB, statisticsReporter)
            val header = fileHeaderCache.getFileHeader(file) { LSMFileFormat.loadFileHeader(driver) }
            val entries = LSMFileCursor(file, header, BlockLoader.basic(driverFactory, statisticsReporter, fileHeaderCache)).use { cursor ->
                cursor.firstOrThrow()
                cursor.ascendingKeySequenceFromHere().fold(0L) { acc, _ -> acc + 1 }
            }

            return entries
        }
    }

    private fun loadHeader(file: VirtualReadWriteFile): FileHeader {
        FileChannelDriver.Factory.withDriver(file) { driver ->
            return LSMFileFormat.loadFileHeader(driver)
        }
    }

    private class Issue(
        val fileName: String,
        val blockIndex: Int?,
        val message: String,
    )

}