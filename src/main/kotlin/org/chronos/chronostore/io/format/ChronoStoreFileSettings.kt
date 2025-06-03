package org.chronos.chronostore.io.format

import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianInt
import org.chronos.chronostore.util.PrefixIO
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.unit.BinarySize
import org.chronos.chronostore.util.unit.BinarySize.Companion.Bytes
import java.io.InputStream
import java.io.OutputStream

class ChronoStoreFileSettings(
    /** The compression used for the blocks in the file. */
    val compression: CompressionAlgorithm,
    /** The maximum size of a single block. */
    val maxBlockSize: BinarySize,
) {

    companion object {

        @JvmStatic
        fun readFrom(inputStream: InputStream): ChronoStoreFileSettings {
            val compressorName = PrefixIO.readBytes(inputStream).asString()
            val maxBlockSize = inputStream.readLittleEndianInt().Bytes

            return ChronoStoreFileSettings(
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
        return "ChronoStoreFileSettings(compression=$compression, maxBlockSize=$maxBlockSize)"
    }
}