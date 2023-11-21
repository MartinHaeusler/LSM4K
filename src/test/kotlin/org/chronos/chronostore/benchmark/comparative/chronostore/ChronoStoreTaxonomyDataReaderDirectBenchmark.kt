package org.chronos.chronostore.benchmark.comparative.chronostore

import org.chronos.chronostore.io.fileaccess.FileChannelDriver
import org.chronos.chronostore.io.format.ChronoStoreFileReader
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystem
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystemSettings
import org.chronos.chronostore.lsm.cache.LocalBlockCache
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
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


        ChronoStoreFileReader(driverFactory.createDriver(inputFile), LocalBlockCache.NONE).use { reader ->
            val timeBeforeRead = System.currentTimeMillis()
            val commands = countCommands(reader)
            val timeAfterRead = System.currentTimeMillis()
            println("Read all ${commands} in file '${inputFile.path}' in ${timeAfterRead - timeBeforeRead}ms.")
            val statistics = ChronoStoreStatistics.snapshot()
            println(statistics.prettyPrint())
        }
    }

    private fun countCommands(reader: ChronoStoreFileReader): Long {
        var commands = 0L
        reader.openCursor().use { cursor ->
            cursor.first()
            val iterator = cursor.ascendingEntrySequenceFromHere().iterator()
            while (iterator.hasNext()) {
                iterator.next()
                // we ignore the actual data here
                commands++
            }
        }
        return commands
    }

}