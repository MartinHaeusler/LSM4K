package org.lsm4k.benchmark.comparative.lsm4k

import org.lsm4k.io.format.CompressionAlgorithm
import org.lsm4k.io.format.LSMFileSettings
import org.lsm4k.io.format.writer.StandardLSMStoreFileWriter
import org.lsm4k.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.lsm4k.io.vfs.disk.DiskBasedVirtualFileSystem
import org.lsm4k.io.vfs.disk.DiskBasedVirtualFileSystemSettings
import org.lsm4k.model.command.Command
import org.lsm4k.util.bytes.Bytes
import org.lsm4k.util.statistics.StatisticsCollector
import org.lsm4k.util.unit.BinarySize.Companion.Bytes
import org.lsm4k.util.unit.BinarySize.Companion.MiB
import java.io.File

object TaxonomyDataWriterDirectBenchmark {

    private val compressionAlgorithm = CompressionAlgorithm.forCompressorName("none")
    private val blockSize = 16.MiB

    @JvmStatic
    fun main(args: Array<String>) {
        val inputFile = File("/home/martin/Documents/lsm4k-test/rawCommandsBinary")
        println("Streaming in data from '${inputFile.absolutePath}' (${inputFile.length().Bytes.toHumanReadableString()})")

        val stats = StatisticsCollector()

        inputFile.inputStream().buffered().use { input ->
            val commandSequence = generateSequence {
                Command.readFromStreamOrNull(input)
            }

            val vfs = DiskBasedVirtualFileSystem(File("/home/martin/Documents/lsm4k-test"), DiskBasedVirtualFileSystemSettings())
            val outputFile = vfs.file("taxonomy_${compressionAlgorithm.compressor.uniqueName.lowercase()}.lsm")

            outputFile.deleteOverWriterFileIfExists()

            val timeBefore = System.currentTimeMillis()
            outputFile.withOverWriter { overWriter ->
                val writer = StandardLSMStoreFileWriter(
                    outputStream = overWriter.outputStream.buffered(),
                    settings = LSMFileSettings(compression = compressionAlgorithm, maxBlockSize = blockSize),
                    statisticsReporter = stats,
                )
                writer.write(numberOfMerges = 0, orderedCommands = commandSequence.iterator(), commandCountEstimate = 3_500_000, maxCompletelyWrittenTSN = 1)
                overWriter.commit()
            }
            val timeAfter = System.currentTimeMillis()
            println("Wrote entries into ${outputFile.path} with ${Bytes.formatSize(outputFile.length)} in ${timeAfter - timeBefore}ms.")
        }
    }


}