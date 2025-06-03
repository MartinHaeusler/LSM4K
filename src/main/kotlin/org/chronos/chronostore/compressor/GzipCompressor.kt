package org.chronos.chronostore.compressor

import org.chronos.chronostore.compressor.api.Compressor
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

/**
 * Uses the GZip algorithm to compress / decompress byte arrays.
 */
class GzipCompressor : Compressor {

    @Suppress("unused")
    constructor() {
        // the default constructor is required for service loader API
    }

    override val uniqueName: String
        get() = "gzip"

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
        return GZIPInputStream(bytes.inputStream()).use<GZIPInputStream, ByteArray> { gzipInput -> gzipInput.readAllBytes() }
    }

    override fun decompress(bytes: ByteArray, offset: Int, length: Int, target: ByteArray) {
        require(bytes.isNotEmpty()) { "An empty byte array cannot be decompressed!" }
        require(offset >= 0) { "Argument 'offset' (${offset}) must not be negative!" }
        require(length >= 0) { "Argument 'length' (${length}) must not be negative!" }
        require(target.isNotEmpty()) { "An empty array cannot be the decompression target!" }
        bytes.inputStream(offset, length).use { rawInput ->
            GZIPInputStream(rawInput).use { gzipInput ->
                gzipInput.readNBytes(target, 0, target.size)
            }
        }
    }

}