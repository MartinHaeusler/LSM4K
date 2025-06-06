package org.lsm4k.compressor

import org.lsm4k.compressor.api.Compressor

/**
 * A "fake" compressor implementation which doesn't actually compress or decompress anything.
 */
class NoCompressor : Compressor {

    @Suppress("unused")
    constructor() {
        // service loader API requires a default constructor
    }

    override val uniqueName: String
        get() = "none"

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

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as NoCompressor

        return uniqueName == other.uniqueName
    }

    override fun hashCode(): Int {
        return javaClass.hashCode()
    }

    override fun toString(): String {
        return "Compressor[none]"
    }

}