package org.chronos.chronostore.io.format.datablock

import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnels
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver
import org.chronos.chronostore.io.format.BlockMetaData
import org.chronos.chronostore.io.format.ChronoStoreFileFormat
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianInt
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.util.*


interface DataBlock {

    val metaData: BlockMetaData

    fun isEmpty(): Boolean

    /**
     * Attempts to get the value for the given [key] from this block.
     *
     * @param key The key-and-timestamp to get from the block.
     *
     * @return Either the successful result, or `null` if the key wasn't found at all.
     *         If the key was found, a pair is returned. The command is the command
     *         associated with the largest key in the block which is less than or equal
     *         to the requested key. The value is a boolean which indicates if the
     *         returned key is the **last** key in the block. If this happens to be
     *         the case, the next data block needs to be consulted as well, because
     *         the entry we returned may not be the **latest** entry for the key.
     */
    fun get(key: KeyAndTimestamp, driver: RandomFileAccessDriver): Pair<Command, Boolean>?

    /**
     * Opens a new cursor on this block.
     *
     * The cursor needs to be closed explicitly.
     *
     * @return the new cursor.
     */
    fun cursor(driver: RandomFileAccessDriver): Cursor<KeyAndTimestamp, Command>

    companion object {

        @JvmStatic
        fun createEagerLoadingInMemoryBlock(
            inputStream: InputStream,
            compressionAlgorithm: CompressionAlgorithm
        ): DataBlock {
            val magicBytes = Bytes(inputStream.readNBytes(ChronoStoreFileFormat.BLOCK_MAGIC_BYTES.size))
            if (magicBytes != ChronoStoreFileFormat.BLOCK_MAGIC_BYTES) {
                throw IllegalArgumentException(
                    "Cannot read block from input: the magic bytes do not match!" +
                        " Expected ${ChronoStoreFileFormat.BLOCK_MAGIC_BYTES.hex()}, found ${magicBytes.hex()}!"
                )
            }
            // read the individual parts of the binary format
            inputStream.readLittleEndianInt() // block size; not needed here
            val blockMetadataSize = inputStream.readLittleEndianInt()
            val blockMetadataBytes = inputStream.readNBytes(blockMetadataSize)

            val bloomFilterSize = inputStream.readLittleEndianInt()
            // skip the bloom filter, we don't need it for eager-loaded blocks
            inputStream.skipNBytes(bloomFilterSize.toLong())

            val blockIndexSize = inputStream.readLittleEndianInt()
            // skip the block index, we don't need it for eager-loaded blocks
            inputStream.skipNBytes(blockIndexSize.toLong())

            val compressedSize = inputStream.readLittleEndianInt()
            val compressedBytes = inputStream.readNBytes(compressedSize)

            // deserialize the binary representations
            val blockMetaData = BlockMetaData.readFrom(ByteArrayInputStream(blockMetadataBytes))

            // decompress the data
            val decompressedData = compressionAlgorithm.decompress(compressedBytes)
            val commands = TreeMap<KeyAndTimestamp, Command>()
            decompressedData.inputStream().use { byteIn ->
                while (byteIn.available() > 0) {
                    val command = Command.readFromStream(byteIn)
                    commands[command.keyAndTimestamp] = command
                }
            }

            return EagerDataBlock(
                metaData = blockMetaData,
                data = commands
            )
        }

        @JvmStatic
        fun createLazyLoadingInMemoryBlock(
            inputStream: InputStream,
            compressionAlgorithm: CompressionAlgorithm
        ): DataBlock {
            val magicBytes = Bytes(inputStream.readNBytes(ChronoStoreFileFormat.BLOCK_MAGIC_BYTES.size))
            if (magicBytes != ChronoStoreFileFormat.BLOCK_MAGIC_BYTES) {
                throw IllegalArgumentException(
                    "Cannot read block from input: the magic bytes do not match!" +
                        " Expected ${ChronoStoreFileFormat.BLOCK_MAGIC_BYTES.hex()}, found ${magicBytes.hex()}!"
                )
            }
            // read the individual parts of the binary format
            inputStream.readLittleEndianInt() // block size; not needed here
            val blockMetadataSize = inputStream.readLittleEndianInt()
            val blockMetadataBytes = inputStream.readNBytes(blockMetadataSize)
            val bloomFilterSize = inputStream.readLittleEndianInt()
            val bloomFilterBytes = inputStream.readNBytes(bloomFilterSize)
            val blockIndexSize = inputStream.readLittleEndianInt()
            val blockIndexBytes = inputStream.readNBytes(blockIndexSize)
            val compressedSize = inputStream.readLittleEndianInt()
            val compressedBytes = inputStream.readNBytes(compressedSize)

            // deserialize the binary representations
            val blockMetaData = BlockMetaData.readFrom(ByteArrayInputStream(blockMetadataBytes))
            val bloomFilter = BloomFilter.readFrom(ByteArrayInputStream(bloomFilterBytes), Funnels.byteArrayFunnel())
            val blockIndex = readBlockIndexFromBytes(blockIndexBytes)

            // decompress the data
            val decompressedData = Bytes(compressionAlgorithm.decompress(compressedBytes))

            return LazyDataBlock(
                metaData = blockMetaData,
                bloomFilter = bloomFilter,
                blockIndex = blockIndex,
                data = decompressedData
            )
        }

        @JvmStatic
        fun createDiskBasedDataBlock(
            inputStream: InputStream,
            compressionAlgorithm: CompressionAlgorithm,
            blockStartOffset: Long,
        ): DataBlock {
            require(compressionAlgorithm == CompressionAlgorithm.NONE) {
                // TODO: check HBase implementation about details how to read partially from a compressed byte array
                "Disk-based data blocks do not support compression at the moment."
            }
            val magicBytes = Bytes(inputStream.readNBytes(ChronoStoreFileFormat.BLOCK_MAGIC_BYTES.size))
            if (magicBytes != ChronoStoreFileFormat.BLOCK_MAGIC_BYTES) {
                throw IllegalArgumentException(
                    "Cannot read block from input: the magic bytes do not match!" +
                        " Expected ${ChronoStoreFileFormat.BLOCK_MAGIC_BYTES.hex()}, found ${magicBytes.hex()}!"
                )
            }
            // read the individual parts of the binary format
            val blockSize = inputStream.readLittleEndianInt()
            val blockMetadataSize = inputStream.readLittleEndianInt()
            val blockMetadataBytes = inputStream.readNBytes(blockMetadataSize)
            val bloomFilterSize = inputStream.readLittleEndianInt()
            val bloomFilterBytes = inputStream.readNBytes(bloomFilterSize)
            val blockIndexSize = inputStream.readLittleEndianInt()
            val blockIndexBytes = inputStream.readNBytes(blockIndexSize)
            val compressedSize = inputStream.readLittleEndianInt()
            // skip over the actual data, we load it lazily
            inputStream.skipNBytes(compressedSize.toLong())

            // deserialize the binary representations
            val blockMetaData = BlockMetaData.readFrom(ByteArrayInputStream(blockMetadataBytes))
            val bloomFilter = BloomFilter.readFrom(ByteArrayInputStream(bloomFilterBytes), Funnels.byteArrayFunnel())
            val blockIndex = readBlockIndexFromBytes(blockIndexBytes)

            return DiskBasedDataBlock(
                metaData = blockMetaData,
                bloomFilter = bloomFilter,
                blockIndex = blockIndex,
                blockStartOffset = blockStartOffset,
                blockEndOffset = blockStartOffset + blockSize
            )
        }

        private fun readBlockIndexFromBytes(blockIndexBytes: ByteArray): NavigableMap<KeyAndTimestamp, Int> {
            blockIndexBytes.inputStream().use { inputStream ->
                val tree = TreeMap<KeyAndTimestamp, Int>()
                while (true) {
                    val keyAndTimestamp = KeyAndTimestamp.readFromStreamOrNull(inputStream)
                        ?: break
                    val offset = inputStream.readLittleEndianInt()
                    tree[keyAndTimestamp] = offset
                }
                return tree
            }
        }

    }

}
