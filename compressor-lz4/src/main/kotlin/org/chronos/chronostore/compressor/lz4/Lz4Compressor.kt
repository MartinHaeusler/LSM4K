package org.chronos.chronostore.compressor.lz4

import net.jpountz.lz4.LZ4FrameInputStream
import net.jpountz.lz4.LZ4FrameOutputStream
import org.chronos.chronostore.compressor.api.Compressor
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

/**
 * Uses the LZ4 algorithm to compress / decompress byte arrays.
 */
class Lz4Compressor : Compressor {

    @Suppress("unused")
    constructor() {
        // the default constructor is required for service loader API
    }

    override val uniqueName: String
        get() = "lz4"

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

}