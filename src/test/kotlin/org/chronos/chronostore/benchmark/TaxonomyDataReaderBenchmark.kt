package org.chronos.chronostore.benchmark

import org.chronos.chronostore.io.fileaccess.MemorySegmentFileDriver
import org.chronos.chronostore.io.format.ChronoStoreFileReader
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystem
import org.chronos.chronostore.lsm.cache.LocalBlockCache
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import java.io.File

object TaxonomyDataReaderBenchmark {

    val inputDir = File("/home/martin/Documents/chronostore-test")
    val inputFileName = "taxonomy_none.chronostore"

    val driverFactory = MemorySegmentFileDriver.Factory

    @JvmStatic
    fun main(args: Array<String>) {
        val vfs = DiskBasedVirtualFileSystem(inputDir)
        val inputFile = vfs.file(inputFileName)

        var commands = 0L
        ChronoStoreFileReader(driverFactory.createDriver(inputFile), LocalBlockCache.NONE).use { reader ->
            val timeBeforeRead = System.currentTimeMillis()
            reader.openCursor().use { cursor ->
                cursor.first()
                val iterator = cursor.ascendingEntrySequenceFromHere().iterator()
                while(iterator.hasNext()){
                    iterator.next()
                    // we ignore the actual data here
                    commands++
                }
            }
            val timeAfterRead = System.currentTimeMillis()
            println("Read all ${commands} in file '${inputFile.path}' in ${timeAfterRead - timeBeforeRead}ms.")
            val statistics = ChronoStoreStatistics.snapshot()
            println(statistics.prettyPrint())
        }
    }

}