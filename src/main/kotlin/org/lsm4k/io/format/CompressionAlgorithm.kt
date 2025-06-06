package org.lsm4k.io.format

import org.lsm4k.compressor.api.Compressor
import org.lsm4k.compressor.api.Compressors
import org.lsm4k.util.bytes.Bytes
import org.lsm4k.util.bytes.Bytes.Companion.compressWith
import org.lsm4k.util.bytes.Bytes.Companion.decompressWith
import org.lsm4k.util.statistics.StatisticsReporter

/**
 * Thin wrapper around a [Compressor] which allows for statistics collection and integration with the [Bytes] interface.
 */
class CompressionAlgorithm(
    val compressor: Compressor,
) {

    // =================================================================================================================
    // STATIC
    // =================================================================================================================

    companion object {

        /**
         * Creates a new [org.lsm4k.io.format.CompressionAlgorithm] using the compressor with the given [compressorName].
         *
         * The compressor name has to correspond (exactly) to the [Compressor.uniqueName] specified by the compressor.
         *
         * @param compressorName The name of the compressor to use.
         *
         * @return The new compression algorithm.
         *
         * @throws IllegalArgumentException if there is no registered compressor for the given [compressorName].
         *                                  If you're confident that the name is correct, please ensure that you
         *                                  have the corresponding library dependency in your build.
         */
        fun forCompressorName(compressorName: String): CompressionAlgorithm {
            val compressor = Compressors.getCompressorForName(compressorName)
            return CompressionAlgorithm(compressor)
        }

    }

    // =================================================================================================================
    // FORWARDING CALLS
    // =================================================================================================================

    fun compress(bytes: Bytes, statisticsReporter: StatisticsReporter): Bytes {
        require(bytes.isNotEmpty()) { "The EMPTY Bytes object cannot be compressed!" }
        return bytes.compressWith(this, statisticsReporter)
    }

    fun decompress(bytes: Bytes, statisticsReporter: StatisticsReporter): Bytes {
        require(bytes.isNotEmpty()) { "The EMPTY Bytes object cannot be decompressed!" }
        return bytes.decompressWith(this, statisticsReporter)
    }

    fun decompress(bytes: Bytes, uncompressedSize: Int, statisticsReporter: StatisticsReporter): Bytes {
        require(bytes.isNotEmpty()) { "The EMPTY Bytes object cannot be decompressed!" }
        require(uncompressedSize > 0) { "The uncompressedSize (${uncompressedSize}) must be greater than zero!" }
        return bytes.decompressWith(this, uncompressedSize, statisticsReporter)
    }

    // =================================================================================================================
    // BASIC CALLS
    // =================================================================================================================

    fun compress(bytes: ByteArray, statisticsReporter: StatisticsReporter): ByteArray {
        val compressed = this.compressor.compress(bytes)
        statisticsReporter.reportCompressionInvocation(bytes.size, compressed.size)
        return compressed
    }

    fun decompress(bytes: ByteArray, statisticsReporter: StatisticsReporter): ByteArray {
        val decompressed = this.compressor.decompress(bytes)
        statisticsReporter.reportDecompressionInvocation(bytes.size, decompressed.size)
        return decompressed
    }

    fun decompress(bytes: ByteArray, offset: Int, length: Int, target: ByteArray, statisticsReporter: StatisticsReporter) {
        this.compressor.decompress(bytes = bytes, offset = offset, length = length, target = target)
        statisticsReporter.reportDecompressionInvocation(length, target.size)
    }

    // =================================================================================================================
    // HASH CODE, EQUALS, TOSTRING
    // =================================================================================================================

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CompressionAlgorithm

        return compressor == other.compressor
    }

    override fun hashCode(): Int {
        return compressor.hashCode()
    }

    override fun toString(): String {
        return "CompressionAlgorithm[${this.compressor.uniqueName}]"
    }

}