package org.chronos.chronostore.io.format

import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianLong
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianLong
import org.chronos.chronostore.util.PrefixIO
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.bytes.Bytes
import java.io.InputStream
import java.io.OutputStream

class BlockMetaData(
    val blockSequenceNumber: Int,
    val minKey: Bytes,
    val maxKey: Bytes,
    val minTimestamp: Timestamp,
    val maxTimestamp: Timestamp,
    val commandCount: Int,
    val compressedDataSize: Int,
    val uncompressedDataSize: Int,
) {

    companion object {

        fun readFrom(inputStream: InputStream): BlockMetaData {
            val blockSequenceNumber = inputStream.readLittleEndianInt()
            val minKey = PrefixIO.readBytes(inputStream)
            val maxKey = PrefixIO.readBytes(inputStream)
            val minTimestamp = inputStream.readLittleEndianLong()
            val maxTimestamp = inputStream.readLittleEndianLong()
            val commandCount = inputStream.readLittleEndianInt()
            val compressedSize = inputStream.readLittleEndianInt()
            val uncompressedSize = inputStream.readLittleEndianInt()
            return BlockMetaData(
                blockSequenceNumber = blockSequenceNumber,
                minKey = minKey,
                maxKey = maxKey,
                minTimestamp = minTimestamp,
                maxTimestamp = maxTimestamp,
                commandCount = commandCount,
                compressedDataSize = compressedSize,
                uncompressedDataSize = uncompressedSize,
            )
        }

    }

    init {
        require(this.blockSequenceNumber >= 0) { "Argument 'blockSequenceNumber' must not be negative (${this.blockSequenceNumber})!" }
        require(!this.minKey.isEmpty()) { "Argument 'minKey' must not be empty!" }
        require(!this.maxKey.isEmpty()) { "Argument 'maxKey' must not be empty!" }
        require(this.minKey <= this.maxKey) { "Argument 'minKey' is greater than argument 'maxKey'!" }
        require(this.minTimestamp >= 0) { "Argument 'minTimestamp' must not be negative!" }
        require(this.maxTimestamp >= 0) { "Argument 'maxTimestamp' must not be negative!" }
        require(this.minTimestamp <= this.maxTimestamp) { "Argument 'minTimestamp' (${this.minTimestamp}) is greater than argument 'maxTimestamp' (${this.maxTimestamp})!" }
        require(this.compressedDataSize >= 0) { "Argument 'compressedSize' must not be negative (${this.compressedDataSize})!" }
        require(this.uncompressedDataSize >= 0) { "Argument 'compressedSize' must not be negative (${this.uncompressedDataSize})!" }
    }

    /**
     * The size of this object in serial form (in bytes) **excluding** the size prefix.
     */
    val byteSize: Int
        get() {
            return Int.SIZE_BYTES + // block sequence number
                Int.SIZE_BYTES + minKey.size + // min key (prefixed by size)
                Int.SIZE_BYTES + maxKey.size + // max key (prefixed by size)
                Long.SIZE_BYTES + // min timestamp
                Long.SIZE_BYTES + // max timestamp
                Int.SIZE_BYTES + // command count
                Int.SIZE_BYTES + // uncompressed size
                Int.SIZE_BYTES  // compressed size
        }

    fun writeTo(outputStream: OutputStream) {
        outputStream.writeLittleEndianInt(this.blockSequenceNumber)
        PrefixIO.writeBytes(outputStream, this.minKey)
        PrefixIO.writeBytes(outputStream, this.maxKey)
        outputStream.writeLittleEndianLong(this.minTimestamp)
        outputStream.writeLittleEndianLong(this.maxTimestamp)
        outputStream.writeLittleEndianInt(this.commandCount)
        outputStream.writeLittleEndianInt(this.compressedDataSize)
        outputStream.writeLittleEndianInt(this.uncompressedDataSize)
    }

    override fun toString(): String {
        return "BlockMetaData(" +
            "minKey=$minKey, " +
            "maxKey=$maxKey, " +
            "minTimestamp=$minTimestamp, " +
            "maxTimestamp=$maxTimestamp, " +
            "commandCount=$commandCount, " +
            "compressedSize=$compressedDataSize, " +
            "uncompressedSize=$uncompressedDataSize" +
            ")"
    }


}
