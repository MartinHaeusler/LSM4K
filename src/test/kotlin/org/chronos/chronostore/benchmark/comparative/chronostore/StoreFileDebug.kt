package org.chronos.chronostore.benchmark.comparative.chronostore

import org.chronos.chronostore.io.fileaccess.FileChannelDriver
import org.chronos.chronostore.io.format.ChronoStoreFileReader
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystem
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystemSettings
import org.chronos.chronostore.lsm.cache.LocalBlockCache
import org.chronos.chronostore.wal.WriteAheadLog
import java.io.File

object StoreFileDebug {

    @JvmStatic
    fun main(args: Array<String>) {
        val inputDir = File("/home/martin/Documents/chronostore-test/taxonomyChronoStore")
        val chronoStoreFileNames = File(inputDir, "data").listFiles().asSequence()
            .filter { it.name.endsWith(".chronostore") }
            .map { it.name }
            .sorted()
            .toList()
        val vfs = DiskBasedVirtualFileSystem(inputDir, DiskBasedVirtualFileSystemSettings())
        var overallEntries = 0
        for(chronoStoreFileName in chronoStoreFileNames) {
            val file = vfs.directory("data").file(chronoStoreFileName)
            FileChannelDriver.Factory.createDriver(file).use { driver ->
                val entries = ChronoStoreFileReader(driver, LocalBlockCache.NONE).openCursor().use { cursor ->
                    cursor.firstOrThrow()
                    cursor.ascendingKeySequenceFromHere().count()
                }
                println("File '${chronoStoreFileName}' has ${entries} entries.")
                overallEntries += entries
            }
        }
        println("=========================================================")
        println("Sum: ${overallEntries} entries in *.chronostore files")

        println()
        println()

        var entriesInWAL = 0
        var transactionCount = 0
        WriteAheadLog(vfs.directory("writeAheadLog")).readWalStreaming { tx ->
            entriesInWAL += tx.storeIdToCommands.values.asSequence().flatten().count()
            transactionCount++
        }
        println("Entries in WAL: ${entriesInWAL}")
        println("Transactions in WAL: ${transactionCount}")

    }


}