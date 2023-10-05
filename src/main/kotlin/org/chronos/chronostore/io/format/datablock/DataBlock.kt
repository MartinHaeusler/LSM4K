package org.chronos.chronostore.io.format.datablock

import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver
import org.chronos.chronostore.io.format.BlockMetaData
import org.chronos.chronostore.io.format.ChronoStoreFileFormat
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianInt
import org.chronos.chronostore.util.TreeMapUtils
import org.chronos.chronostore.util.bytes.BytesBuffer
import org.chronos.chronostore.util.cursor.Cursor
import java.io.ByteArrayInputStream
import java.io.InputStream


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
        @Suppress("UNCHECKED_CAST")
        fun createEagerLoadingInMemoryBlock(
            input: Bytes,
            compressionAlgorithm: CompressionAlgorithm
        ): DataBlock {
            val buffer = BytesBuffer(input)

            val magicBytes = buffer.takeBytes(ChronoStoreFileFormat.BLOCK_MAGIC_BYTES.size)
            if (magicBytes != ChronoStoreFileFormat.BLOCK_MAGIC_BYTES) {
                throw IllegalArgumentException(
                    "Cannot read block from input: the magic bytes do not match!" +
                        " Expected ${ChronoStoreFileFormat.BLOCK_MAGIC_BYTES.hex()}, found ${magicBytes}!"
                )
            }
            // read the individual parts of the binary format
            buffer.takeLittleEndianInt() // block size; not needed here
            val blockMetadataSize = buffer.takeLittleEndianInt()
            val blockMetadataBytes = buffer.takeBytes(blockMetadataSize)

            val bloomFilterSize = buffer.takeLittleEndianInt()
            // skip the bloom filter, we don't need it for eager-loaded blocks
            buffer.skipBytes(bloomFilterSize)

            val compressedSize = buffer.takeLittleEndianInt()
            val compressedBytes = buffer.takeBytes(compressedSize)

            // deserialize the binary representations
            val blockMetaData = blockMetadataBytes.createInputStream().use(BlockMetaData::readFrom)

            // decompress the data
            val decompressedData = compressionAlgorithm.decompress(compressedBytes)

            // we use "ArrayList" instead of "mutableListOf(...)" here because we can provide an initial size

            val commandsArray = arrayOfNulls<Map.Entry<KeyAndTimestamp, Command>>(blockMetaData.commandCount)
            val decompressedBuffer = BytesBuffer(decompressedData)

            for (i in 0..commandsArray.lastIndex) {
                val command = Command.readFromBytesBuffer(decompressedBuffer)
                    ?: throw IllegalStateException(
                        "Could not read command at index ${i} from the decompressed buffer -" +
                            " expected ${blockMetaData.commandCount} commands to be in the buffer!"
                    )
                commandsArray[i] = ImmutableEntry(command.keyAndTimestamp, command)
            }

            // this cast is 100% safe!
            // We collect the data in an array of commands which gets initialized with NULL values. Once we've extracted
            // all commands from the block, we will have filled every spot in the array with a non-NULL command.
            val commands = TreeMapUtils.treeMapFromSortedList(commandsArray.asList() as List<Map.Entry<KeyAndTimestamp, Command>>)

            return EagerDataBlock(
                metaData = blockMetaData,
                data = commands
            )

        }

        @JvmStatic
        fun createEagerLoadingInMemoryBlock(
            inputStream: InputStream,
            compressionAlgorithm: CompressionAlgorithm
        ): DataBlock {
            val magicBytes = Bytes.wrap(inputStream.readNBytes(ChronoStoreFileFormat.BLOCK_MAGIC_BYTES.size))
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

            val compressedSize = inputStream.readLittleEndianInt()
            val compressedBytes = inputStream.readNBytes(compressedSize)

            // deserialize the binary representations
            val blockMetaData = BlockMetaData.readFrom(ByteArrayInputStream(blockMetadataBytes))

            // decompress the data
            val decompressedData = compressionAlgorithm.decompress(compressedBytes)

            // we use "ArrayList" instead of "mutableListOf(...)" here because we can provide an initial size
            val commandsList = ArrayList<Map.Entry<KeyAndTimestamp, Command>>(blockMetaData.commandCount)

            decompressedData.inputStream().use { byteIn ->
                while (byteIn.available() > 0) {
                    val command = Command.readFromStream(byteIn)
                    commandsList += ImmutableEntry(command.keyAndTimestamp, command)
                }
            }

            val commands = TreeMapUtils.treeMapFromSortedList(commandsList)

            return EagerDataBlock(
                metaData = blockMetaData,
                data = commands
            )
        }

    }


    private class ImmutableEntry<K, V>(
        override val key: K,
        override val value: V,
    ) : Map.Entry<K, V>

}
