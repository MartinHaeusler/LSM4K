package org.chronos.chronostore.benchmark

import com.fasterxml.jackson.databind.ObjectMapper
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.format.ChronoStoreFileWriter
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystem
import org.chronos.chronostore.util.Bytes
import java.io.File

object TaxonomyDataBenchmark {

    @JvmStatic
    fun main(args: Array<String>) {
        println("Reading, converting and sorting input data...")
        val objectMapper = ObjectMapper()
        val orderedCommands = File("/home/martin/Documents/chronos-ng-testdata/export.jsonl").bufferedReader().lineSequence()
            .map { objectMapper.readTree(it) }
            .map {
                val key = it["key"].asText()
                val timestamp = it["timestamp"].asLong()
                val value = it["value"]
                if (value.isNull) {
                    Command.del(Bytes(key), timestamp)
                } else {
                    Command.put(Bytes(key), timestamp, Bytes(objectMapper.writeValueAsBytes(value)))
                }
            }
            .sorted()
            .toList()

        println("Writing file (${orderedCommands.size} entries)")

        val vfs = DiskBasedVirtualFileSystem(File("/home/martin/Documents/chronostore-test"))
        val outputFile = vfs.file("taxonomy_lzo1x.chronostore")

        val timeBefore = System.currentTimeMillis()
        outputFile.withOverWriter { overWriter ->
            val writer = ChronoStoreFileWriter(
                outputStream = overWriter.outputStream.buffered(),
                settings = ChronoStoreFileSettings(CompressionAlgorithm.LZO_1X, 1024 * 1024 * 16, 100),
                metadata = emptyMap()
            )
            writer.writeFile(orderedCommands.iterator())
            overWriter.commit()
        }
        val timeAfter = System.currentTimeMillis()
        println("Wrote ${orderedCommands.size} entries into file with ${Bytes.formatSize(outputFile.length)} in ${timeAfter - timeBefore}ms.")
    }


//    @JvmStatic
//    fun main(args: Array<String>) {
//        val vfs = DiskBasedVirtualFileSystem(File("/home/martin/Documents/chronostore-test"))
//        val inputFile = vfs.file("output.chronostore")
//
//        val timeBefore = System.currentTimeMillis()
//        val timeAfterOpen: Long
//
//        val driverFactory = MemorySegmentFileDriver.Factory()
//        driverFactory.createDriver(inputFile).use { driver ->
//            ChronoStoreFileReader(driver, BlockReadMode.DISK_BASED).use { reader ->
//                reader.fileHeader
//                timeAfterOpen = System.currentTimeMillis()
//            }
//        }
//
//        val timeAfter = System.currentTimeMillis()
//        println("Opened file in ${timeAfterOpen - timeBefore}ms.")
//    }



}