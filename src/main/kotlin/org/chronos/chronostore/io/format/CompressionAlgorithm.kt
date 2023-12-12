package org.chronos.chronostore.io.format

import com.github.luben.zstd.Zstd
import net.jpountz.lz4.LZ4FrameInputStream
import net.jpountz.lz4.LZ4FrameOutputStream
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.bytes.Bytes.Companion.compressWith
import org.chronos.chronostore.util.bytes.Bytes.Companion.decompressWith
import org.xerial.snappy.Snappy
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
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

        override fun decompress(bytes: ByteArray, offset: Int, length: Int, target: ByteArray) {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be decompressed!" }
            require(offset >= 0) { "Argument 'offset' (${offset}) must not be negative!" }
            require(length >= 0) { "Argument 'length' (${length}) must not be negative!" }
            require(target.isNotEmpty()) { "An empty array cannot be the decompression target!" }
            System.arraycopy(bytes, offset, target, 0, length)
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

        override fun decompress(bytes: ByteArray, offset: Int, length: Int, target: ByteArray) {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be decompressed!" }
            require(offset >= 0) { "Argument 'offset' (${offset}) must not be negative!" }
            require(length >= 0) { "Argument 'length' (${length}) must not be negative!" }
            require(target.isNotEmpty()) { "An empty array cannot be the decompression target!" }
            Snappy.uncompress(bytes, offset, length, target, 0)
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

        override fun decompress(bytes: ByteArray, offset: Int, length: Int, target: ByteArray) {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be decompressed!" }
            require(offset >= 0) { "Argument 'offset' (${offset}) must not be negative!" }
            require(length >= 0) { "Argument 'length' (${length}) must not be negative!" }
            require(target.isNotEmpty()) { "An empty array cannot be the decompression target!" }
            bytes.inputStream(offset, length).use { gzipInput -> gzipInput.readNBytes(target, 0, target.size) }
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

        override fun decompress(bytes: ByteArray, offset: Int, length: Int, target: ByteArray) {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be decompressed!" }
            require(offset >= 0) { "Argument 'offset' (${offset}) must not be negative!" }
            require(length >= 0) { "Argument 'length' (${length}) must not be negative!" }
            require(target.isNotEmpty()) { "An empty array cannot be the decompression target!" }
            Zstd.decompress(ByteBuffer.wrap(target), ByteBuffer.wrap(bytes, offset, length))
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

        override fun decompress(bytes: ByteArray, offset: Int, length: Int, target: ByteArray) {
            require(bytes.isNotEmpty()) { "An empty byte array cannot be decompressed!" }
            require(offset >= 0) { "Argument 'offset' (${offset}) must not be negative!" }
            require(length >= 0) { "Argument 'length' (${length}) must not be negative!" }
            require(target.isNotEmpty()) { "An empty array cannot be the decompression target!" }
            ByteArrayInputStream(bytes, offset, length).use { bais ->
                LZ4FrameInputStream(bais).use { lz4In ->
                    lz4In.readNBytes(target, 0, target.size)
                }
            }
        }

    },

    ;

    companion object {

        fun fromAlgorithmIndex(algorithmIndex: Int): CompressionAlgorithm {
            for (algorithm in entries) {
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

    fun decompress(bytes: Bytes, uncompressedSize: Int): Bytes {
        require(bytes.isNotEmpty()) { "The EMPTY Bytes object cannot be decompressed!" }
        require(uncompressedSize > 0) { "The uncompressedSize (${uncompressedSize}) must be greater than zero!" }
        return bytes.decompressWith(this, uncompressedSize)
    }

    abstract fun decompress(bytes: ByteArray): ByteArray

    abstract fun decompress(bytes: ByteArray, offset: Int, length: Int, target: ByteArray)

}