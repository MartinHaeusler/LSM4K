package org.lsm4k.compressor.zstd

import com.github.luben.zstd.Zstd
import org.lsm4k.compressor.api.Compressor
import java.nio.ByteBuffer

/**
 * Uses the ZSTD algorithm to compress / decompress byte arrays.
 */
class ZstdCompressor : Compressor {

    @Suppress("unused")
    constructor() {
        // the default constructor is required for service loader API
    }

    override val uniqueName: String
        get() = "zstd"

    override fun compress(bytes: ByteArray): ByteArray {
        require(bytes.isNotEmpty()) { "An empty byte array cannot be compressed!" }
        return Zstd.compress(bytes)
    }

    @Suppress("DEPRECATION") // API has no replacement for "decompressedSize()"...
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

}