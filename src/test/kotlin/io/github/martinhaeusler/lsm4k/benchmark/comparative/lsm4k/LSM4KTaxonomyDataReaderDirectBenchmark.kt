package io.github.martinhaeusler.lsm4k.benchmark.comparative.lsm4k

import io.github.martinhaeusler.lsm4k.io.fileaccess.FileChannelDriver
import io.github.martinhaeusler.lsm4k.io.format.BlockLoader
import io.github.martinhaeusler.lsm4k.io.format.LSMFileFormat
import io.github.martinhaeusler.lsm4k.io.format.cursor.LSMFileCursor
import io.github.martinhaeusler.lsm4k.io.vfs.disk.DiskBasedVirtualFileSystem
import io.github.martinhaeusler.lsm4k.io.vfs.disk.DiskBasedVirtualFileSystemSettings
import io.github.martinhaeusler.lsm4k.lsm.cache.FileHeaderCache
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsCollector
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.MiB
import java.io.File

object LSM4KTaxonomyDataReaderDirectBenchmark {

    val inputDir = File("/home/martin/Documents/lsm4k-test")
    val inputFileName = "taxonomy_snappy.lsm"

    val driverFactory = FileChannelDriver.Factory

    @JvmStatic
    fun main(args: Array<String>) {

        println("Attach profiler now! Press any key to continue")
        System.`in`.read()

        println("STARTING BENCHMARK")

        val vfs = DiskBasedVirtualFileSystem(inputDir, DiskBasedVirtualFileSystemSettings())
        val inputFile = vfs.file(inputFileName)


        this.driverFactory.createDriver(inputFile).use { driver ->
            val stats = StatisticsCollector.active()
            val fileHeaderCache = FileHeaderCache.create(100.MiB, stats)
            val header = fileHeaderCache.getFileHeader(inputFile) { LSMFileFormat.loadFileHeader(driver) }
            LSMFileCursor(inputFile, header, BlockLoader.basic(this.driverFactory, stats, fileHeaderCache)).use { cursor ->
                val timeBeforeRead = System.currentTimeMillis()
                val commands = countCommands(cursor)
                val timeAfterRead = System.currentTimeMillis()
                println("Read all ${commands} in file '${inputFile.path}' in ${timeAfterRead - timeBeforeRead}ms.")
                println(stats.report()!!.prettyPrint())
            }
        }
    }

    private fun countCommands(cursor: LSMFileCursor): Long {
        var commands = 0L
        cursor.first()
        val iterator = cursor.ascendingEntrySequenceFromHere().iterator()
        while (iterator.hasNext()) {
            iterator.next()
            // we ignore the actual data here
            commands++
        }
        return commands
    }

}