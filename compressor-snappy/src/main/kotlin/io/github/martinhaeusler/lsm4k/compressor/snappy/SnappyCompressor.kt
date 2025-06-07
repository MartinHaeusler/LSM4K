package io.github.martinhaeusler.lsm4k.compressor.snappy

import io.github.martinhaeusler.lsm4k.compressor.api.Compressor
import org.xerial.snappy.Snappy

/**
 * Uses the Snappy algorithm to compress / decompress byte arrays.
 */
class SnappyCompressor : Compressor {

    companion object {

        init {
            // get initialization out of the way
            Snappy.getNativeLibraryVersion()
        }

        @JvmStatic
        val UNIQUE_NAME = "snappy"

    }

    @Suppress("unused")
    constructor() {
        // the default constructor is required for service loader API
    }

    override val uniqueName: String
        get() = UNIQUE_NAME

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

}