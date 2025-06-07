package io.github.martinhaeusler.lsm4k.compressor.api

/**
 * Compresses and decompresses byte arrays.
 */
interface Compressor {

    /** The unique name of the compressor. */
    val uniqueName: String

    /**
     * Compresses the given [bytes].
     *
     * @param bytes The bytes to compress
     *
     * @return the compressed bytes
     */
    fun compress(bytes: ByteArray): ByteArray

    /**
     * Decompresses the given [bytes].
     *
     * @param bytes The bytes to decompress
     *
     * @return the decompressed bytes
     */
    fun decompress(bytes: ByteArray): ByteArray

    /**
     * Decompresses the section of the given [bytes] that starts at [offset] and has the given [length].
     *
     * @param bytes The bytes to decompress
     * @param offset The starting offset in the [bytes] where the sequence starts (zero-based, inclusive)
     * @param length The length of the section in the [bytes] which should be decompressed. Not negative.
     * @param target The byte array to receive the decompressed data.
     */
    fun decompress(bytes: ByteArray, offset: Int, length: Int, target: ByteArray)

}