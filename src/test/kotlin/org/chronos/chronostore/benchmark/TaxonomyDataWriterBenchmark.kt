package org.chronos.chronostore.benchmark

import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.format.ChronoStoreFileWriter
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystem
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.unit.Bytes
import org.chronos.chronostore.util.unit.MiB
import java.io.File

object TaxonomyDataWriterBenchmark {

    val compressionAlgorithm = CompressionAlgorithm.NONE
    val blockSize = 16.MiB

    @JvmStatic
    fun main(args: Array<String>) {
        val inputFile = File("/home/martin/Documents/chronostore-test/rawCommandsBinary")
        println("Streaming in data from '${inputFile.absolutePath}' (${inputFile.length().Bytes.toHumanReadableString()})")

        inputFile.inputStream().buffered().use { input ->
            val commandSequence = generateSequence {
                Command.readFromStreamOrNull(input)
            }

            val vfs = DiskBasedVirtualFileSystem(File("/home/martin/Documents/chronostore-test"))
            val outputFile = vfs.file("taxonomy_${compressionAlgorithm.name.lowercase()}.chronostore")

            outputFile.deleteOverWriterFileIfExists()

            val timeBefore = System.currentTimeMillis()
            outputFile.withOverWriter { overWriter ->
                val writer = ChronoStoreFileWriter(
                    outputStream = overWriter.outputStream.buffered(),
                    settings = ChronoStoreFileSettings(compression = compressionAlgorithm, maxBlockSize = blockSize),
                    metadata = emptyMap()
                )
                writer.writeFile(0, commandSequence.iterator())
                overWriter.commit()
            }
            val timeAfter = System.currentTimeMillis()
            println("Wrote entries into ${outputFile.path} with ${Bytes.formatSize(outputFile.length)} in ${timeAfter - timeBefore}ms.")
        }
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