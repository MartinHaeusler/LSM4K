package org.chronos.chronostore.benchmark

import net.jpountz.lz4.LZ4Compressor
import net.jpountz.lz4.LZ4Factory
import net.jpountz.lz4.LZ4FastDecompressor
import org.chronos.chronostore.benchmark.util.Statistics.Companion.statistics
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.unit.MiB
import org.xerial.snappy.Snappy
import java.io.File
import kotlin.system.measureTimeMillis


object CompressionBenchmark {

    val lz4NativeFactory = LZ4Factory.nativeInstance()
    val lz4JavaFactory = LZ4Factory.unsafeInstance()

    init {
        Snappy.getNativeLibraryVersion()
    }


    @JvmStatic
    fun main(args: Array<String>) {
        measureSnappy()
    }


    fun measureSnappy() {
        this.measureCompress("snappy", ::compressSnappy)
        this.measureDecompress("snappy", ::compressSnappy, ::decompressSnappy)
    }

    fun measureLZ4Native() {
        this.measureCompress("lz4-native", ::compressLZ4Native)
        this.measureDecompress("lz4-native", ::compressLZ4Native, ::decompressLZ4Native)
    }

    fun measureLZ4Java() {
        this.measureCompress("lz4-java", ::compressLZ4Java)
        this.measureDecompress("lz4-java", ::compressLZ4Java, ::decompressLZ4Java)
    }


    private fun measureCompress(name: String, compress: (ByteArray) -> ByteArray) {
        val uncompressedData = this.prepareData()

        println("Compressed size [${name}]: ${compress(uncompressedData).size} Bytes")

        // prewarm
        println("Prewarming [${name}] compress.")
        repeat(1000) {
            compress(uncompressedData)
        }

        // measure
        println("Testing [${name}] compress.")
        val dataPoints = ArrayList<Long>(1000)
        var blackHole = 0L
        repeat(100) {
            measureTimeMillis {
                repeat(10){
                    blackHole += compress(uncompressedData).size
                }
            }.let { dataPoints += it }
        }
        println()
        println(dataPoints.statistics().prettyPrint("Compress [${name}]"))
        println()
        println("Black Hole: ${blackHole}")
    }

    private fun measureDecompress(name: String, compress: (ByteArray)->ByteArray, decompress: (ByteArray, Int) -> ByteArray) {
        val uncompressedData = this.prepareData()
        val compressedData = compress(uncompressedData)

        // prewarm
        println("Prewarming [${name}] decompress.")
        repeat(1000) {
            decompress(compressedData, uncompressedData.size)
        }

        // measure
        println("Testing [${name}] decompress.")
        val dataPoints = ArrayList<Long>(1000)
        var blackHole = 0L
        repeat(100) {
            measureTimeMillis {
                repeat(10){
                    blackHole += decompress(compressedData, uncompressedData.size).size
                }
            }.let { dataPoints += it }
        }
        println()
        println(dataPoints.statistics().prettyPrint("Decompress [${name}]"))
        println()
        println("Black Hole: ${blackHole}")
    }


    private fun prepareData(): ByteArray {
        val dataFile = File("/home/martin/Documents/chronostore-test/rawCommandsBinary")
        val allBytes = mutableListOf<Bytes>()
        var currentSizeInBytes = 0

        dataFile.inputStream().use { inputStream ->
            while (true) {
                val command = Command.readFromStreamOrNull(inputStream)
                    ?: break

                val bytes = command.toBytes()
                if (bytes.size + currentSizeInBytes > 8.MiB.bytes) {
                    break
                }
                allBytes += bytes
                currentSizeInBytes += bytes.size
            }
        }

        val newByteArray = allBytes.flatten().toByteArray()
        println("Working with ${newByteArray.size} bytes of data.")
        return newByteArray
    }

    private fun compressLZ4Native(byteArray: ByteArray): ByteArray {
        val compressor: LZ4Compressor = lz4NativeFactory.fastCompressor()
        val maxCompressedLength = compressor.maxCompressedLength(byteArray.size)
        val compressed = ByteArray(maxCompressedLength)
        compressor.compress(byteArray, 0, byteArray.size, compressed, 0, maxCompressedLength)
        return compressed
    }

    private fun decompressLZ4Native(compressed: ByteArray, decompressedSize: Int): ByteArray {
        val decompressor: LZ4FastDecompressor = lz4NativeFactory.fastDecompressor()
        val restored = ByteArray(decompressedSize)
        decompressor.decompress(compressed, 0, restored, 0, decompressedSize)
        return restored
    }

    private fun compressLZ4Java(byteArray: ByteArray): ByteArray {
        val compressor: LZ4Compressor = lz4JavaFactory.fastCompressor()
        val maxCompressedLength = compressor.maxCompressedLength(byteArray.size)
        val compressed = ByteArray(maxCompressedLength)
        compressor.compress(byteArray, 0, byteArray.size, compressed, 0, maxCompressedLength)
        return compressed
    }

    private fun decompressLZ4Java(compressed: ByteArray, decompressedSize: Int): ByteArray {
        val decompressor: LZ4FastDecompressor = lz4JavaFactory.fastDecompressor()
        val restored = ByteArray(decompressedSize)
        decompressor.decompress(compressed, 0, restored, 0, decompressedSize)
        return restored
    }

    private fun compressSnappy(byteArray: ByteArray): ByteArray {
        return Snappy.compress(byteArray)
    }

    private fun decompressSnappy(compressed: ByteArray, decompressedSize: Int): ByteArray {
        return Snappy.uncompress(compressed)
    }

}