package org.chronos.chronostore.benchmark.comparative.chronostore

import org.chronos.chronostore.io.fileaccess.FileChannelDriver
import org.chronos.chronostore.io.format.BlockLoader
import org.chronos.chronostore.io.format.ChronoStoreFileFormat
import org.chronos.chronostore.io.format.cursor.ChronoStoreFileCursor
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystem
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystemSettings
import org.chronos.chronostore.lsm.cache.FileHeaderCache
import org.chronos.chronostore.util.statistics.StatisticsCollector
import org.chronos.chronostore.util.unit.BinarySize.Companion.MiB
import org.xerial.snappy.Snappy
import java.io.File

object ChronoStoreTaxonomyDataReaderDirectBenchmark {

    val inputDir = File("/home/martin/Documents/chronostore-test")
    val inputFileName = "taxonomy_snappy.chronostore"

    val driverFactory = FileChannelDriver.Factory

    @JvmStatic
    fun main(args: Array<String>) {
        // access snappy to get native initialization out of the way.
        // This only happens once per JVM restart and we don't want to include it in the benchmark.
        Snappy.getNativeLibraryVersion()

        println("Attach profiler now! Press any key to continue")
        System.`in`.read()

        println("STARTING BENCHMARK")

        val vfs = DiskBasedVirtualFileSystem(inputDir, DiskBasedVirtualFileSystemSettings())
        val inputFile = vfs.file(inputFileName)


        this.driverFactory.createDriver(inputFile).use { driver ->
            val stats = StatisticsCollector.active()
            val fileHeaderCache = FileHeaderCache.create(100.MiB, stats)
            val header = fileHeaderCache.getFileHeader(inputFile) { ChronoStoreFileFormat.loadFileHeader(driver) }
            ChronoStoreFileCursor(inputFile, header, BlockLoader.basic(this.driverFactory, stats, fileHeaderCache)).use { cursor ->
                val timeBeforeRead = System.currentTimeMillis()
                val commands = countCommands(cursor)
                val timeAfterRead = System.currentTimeMillis()
                println("Read all ${commands} in file '${inputFile.path}' in ${timeAfterRead - timeBeforeRead}ms.")
                println(stats.report()!!.prettyPrint())
            }
        }
    }

    private fun countCommands(cursor: ChronoStoreFileCursor): Long {
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