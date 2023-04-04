package org.chronos.chronostore.io.format

import org.anarres.lzo.LzoAlgorithm
import org.anarres.lzo.LzoInputStream
import org.anarres.lzo.LzoLibrary
import org.anarres.lzo.LzoOutputStream
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.Bytes.Companion.compressWith
import org.chronos.chronostore.util.Bytes.Companion.decompressWith
import org.xerial.snappy.Snappy
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream


enum class CompressionAlgorithm(
    val algorithmIndex: Int
) {

    NONE(0) {

        override fun compress(bytes: ByteArray): ByteArray {
            // for consistency with the other algorithms, we do this check here,
            // even though it is of no consequence for this method.
            require(bytes.isNotEmpty()) { "An empty byte array cannot be compressed!" }
            // no-op by definition.
            return bytes
        }

        override fun decompress(bytes: ByteArray): ByteArray {
            // for consistency with the other algorithms, we do this check here,
            // even though it is of no consequence for this method.
            require(bytes.isNotEmpty()) { "An empty byte array cannot be decompressed!" }
            // no-op by definition.
            return bytes
        }

    },

    SNAPPY(10) {

        override fun compress(bytes: ByteArray): ByteArray {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be compressed!" }
            return Snappy.compress(bytes)
        }

        override fun decompress(bytes: ByteArray): ByteArray {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be decompressed!" }
            return Snappy.uncompress(bytes)
        }

    },

    LZO_1(20) {

        override fun compress(bytes: ByteArray): ByteArray {
            return compressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1)
        }

        override fun decompress(bytes: ByteArray): ByteArray {
            return decompressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1)
        }

    },

    LZO_1A(21) {

        override fun compress(bytes: ByteArray): ByteArray {
            return compressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1A)
        }

        override fun decompress(bytes: ByteArray): ByteArray {
            return decompressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1A)
        }

    },

    LZO_1B(22) {

        override fun compress(bytes: ByteArray): ByteArray {
            return compressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1B)
        }

        override fun decompress(bytes: ByteArray): ByteArray {
            return decompressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1B)
        }

    },

    LZO_1C(23) {

        override fun compress(bytes: ByteArray): ByteArray {
            return compressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1C)
        }

        override fun decompress(bytes: ByteArray): ByteArray {
            return decompressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1C)
        }

    },

    LZO_1F(24) {

        override fun compress(bytes: ByteArray): ByteArray {
            return compressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1F)
        }

        override fun decompress(bytes: ByteArray): ByteArray {
            return decompressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1F)
        }

    },

    LZO_1X(25) {

        override fun compress(bytes: ByteArray): ByteArray {
            return compressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1X)
        }

        override fun decompress(bytes: ByteArray): ByteArray {
            return decompressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1X)
        }

    },

    LZO_1Y(26) {

        override fun compress(bytes: ByteArray): ByteArray {
            return compressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1Y)
        }

        override fun decompress(bytes: ByteArray): ByteArray {
            return decompressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1Y)
        }

    },

    LZO_1Z(27) {

        override fun compress(bytes: ByteArray): ByteArray {
            return compressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1Z)
        }

        override fun decompress(bytes: ByteArray): ByteArray {
            return decompressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO1Z)
        }

    },

    LZO_2A(28) {

        override fun compress(bytes: ByteArray): ByteArray {
            return compressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO2A)
        }

        override fun decompress(bytes: ByteArray): ByteArray {
            return decompressWithLzoAlgorithm(bytes, LzoAlgorithm.LZO2A)
        }

    },

    GZIP(40) {

        override fun compress(bytes: ByteArray): ByteArray {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be compressed!" }
            // we expect GZIP to have a compression rate of about 50%, so we provide
            // half the size of the input to the output stream as initial size.
            val baos = ByteArrayOutputStream(bytes.size / 2)
            GZIPOutputStream(baos).use { gzipOut -> gzipOut.write(bytes) }
            return baos.toByteArray()
        }

        override fun decompress(bytes: ByteArray): ByteArray {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be decompressed!" }
            return GZIPInputStream(bytes.inputStream()).use { gzipInput -> gzipInput.readAllBytes() }
        }

    },

    ;

    companion object {

        private fun compressWithLzoAlgorithm(bytes: ByteArray, lzoAlgorithm: LzoAlgorithm): ByteArray {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be compressed!" }
            // we expect LZO to have a compression rate of about 50%, so we provide
            // half the size of the input to the output stream as initial size.
            val out = ByteArrayOutputStream(bytes.size / 2)
            val compressor = LzoLibrary.getInstance().newCompressor(lzoAlgorithm, null)
            LzoOutputStream(out, compressor).use { lzoOutput -> lzoOutput.write(bytes) }
            return out.toByteArray()
        }

        private fun decompressWithLzoAlgorithm(bytes: ByteArray, lzoAlgorithm: LzoAlgorithm): ByteArray {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be decompressed!" }
            val decompressor = LzoLibrary.getInstance().newDecompressor(lzoAlgorithm, null)
            return LzoInputStream(bytes.inputStream(), decompressor).use { lzoInput -> lzoInput.readAllBytes() }
        }

        fun fromAlgorithmIndex(algorithmIndex: Int): CompressionAlgorithm {
            for (algorithm in values()) {
                if (algorithm.algorithmIndex == algorithmIndex) {
                    return algorithm
                }
            }
            throw IllegalArgumentException("Could not find CompressionAlgorithm for int ${algorithmIndex}!")
        }

    }

    fun compress(bytes: Bytes): Bytes {
        require(bytes.isNotEmpty()) { "The EMPTY Bytes object cannot be compressed!" }
        return bytes.compressWith(this)
    }

    abstract fun compress(bytes: ByteArray): ByteArray

    fun decompress(bytes: Bytes): Bytes {
        require(bytes.isNotEmpty()) { "The EMPTY Bytes object cannot be decompressed!" }
        return bytes.decompressWith(this)
    }

    abstract fun decompress(bytes: ByteArray): ByteArray

}