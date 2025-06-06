package org.lsm4k.io.format

import org.lsm4k.util.LittleEndianExtensions.readLittleEndianInt
import org.lsm4k.util.LittleEndianExtensions.writeLittleEndianInt
import org.lsm4k.util.PrefixIO
import org.lsm4k.util.bytes.Bytes
import org.lsm4k.util.unit.BinarySize
import org.lsm4k.util.unit.BinarySize.Companion.Bytes
import java.io.InputStream
import java.io.OutputStream

class LSMFileSettings(
    /** The compression used for the blocks in the file. */
    val compression: CompressionAlgorithm,
    /** The maximum size of a single block. */
    val maxBlockSize: BinarySize,
) {

    companion object {

        @JvmStatic
        fun readFrom(inputStream: InputStream): LSMFileSettings {
            val compressorName = PrefixIO.readBytes(inputStream).asString()
            val maxBlockSize = inputStream.readLittleEndianInt().Bytes

            return LSMFileSettings(
                compression = CompressionAlgorithm.forCompressorName(compressorName),
                maxBlockSize = maxBlockSize,
            )
        }

    }

    val sizeBytes: Int
        get() {
            return Int.SIZE_BYTES + // compression algorithm name length
                Bytes.of(this.compression.compressor.uniqueName).size + // compression algorithm name content
                Int.SIZE_BYTES // size of "maxBlockSize"
        }

    fun writeTo(outputStream: OutputStream) {
        PrefixIO.writeBytes(outputStream, Bytes.of(this.compression.compressor.uniqueName))
        outputStream.writeLittleEndianInt(this.maxBlockSize.bytes.toInt())
    }

    override fun toString(): String {
        return "LSMFileSettings(compression=$compression, maxBlockSize=$maxBlockSize)"
    }
}