package org.chronos.chronostore.io.format

import com.github.luben.zstd.Zstd
import net.jpountz.lz4.LZ4FrameInputStream
import net.jpountz.lz4.LZ4FrameOutputStream
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.Bytes.Companion.compressWith
import org.chronos.chronostore.util.Bytes.Companion.decompressWith
import org.xerial.snappy.Snappy
import java.io.ByteArrayInputStream
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

    ZSTD(50) {

        override fun compress(bytes: ByteArray): ByteArray {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be compressed!" }
            return Zstd.compress(bytes)
        }

        override fun decompress(bytes: ByteArray): ByteArray {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be decompressed!" }
            return Zstd.decompress(bytes, Zstd.decompressedSize(bytes).toInt())
        }

    },

    LZ4(60) {

        override fun compress(bytes: ByteArray): ByteArray {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be compressed!" }
            ByteArrayOutputStream().use { baos ->
                LZ4FrameOutputStream(baos).use { lz4Out ->
                    lz4Out.write(bytes)
                }
                return baos.toByteArray()
            }
        }

        override fun decompress(bytes: ByteArray): ByteArray {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be compressed!" }
            ByteArrayInputStream(bytes).use { bais ->
                LZ4FrameInputStream(bais).use { lz4In ->
                    return lz4In.readAllBytes()
                }
            }
        }

    },

    ;

    companion object {

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