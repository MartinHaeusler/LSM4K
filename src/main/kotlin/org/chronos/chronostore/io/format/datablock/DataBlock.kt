package org.chronos.chronostore.io.format.datablock

import org.chronos.chronostore.io.format.BlockMetaData
import org.chronos.chronostore.io.format.ChronoStoreFileFormat
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTSN
import org.chronos.chronostore.util.TreeMapUtils
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.bytes.BytesBuffer
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.EmptyCursor
import org.chronos.chronostore.util.cursor.NavigableMapCursor
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import java.util.*

class DataBlock(
    val metaData: BlockMetaData,
    private val data: NavigableMap<KeyAndTSN, Command>,
) {

    companion object {

        @JvmStatic
        @Suppress("UNCHECKED_CAST")
        fun loadBlock(
            input: Bytes,
            compressionAlgorithm: CompressionAlgorithm
        ): DataBlock {
            ChronoStoreStatistics.BLOCK_LOADS.incrementAndGet()

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

            val compressedSize = buffer.takeLittleEndianInt()
            val compressedBytes = buffer.takeBytes(compressedSize)

            // deserialize the binary representations
            val blockMetaData = blockMetadataBytes.createInputStream().use(BlockMetaData::readFrom)

            // decompress the data
            val decompressedData = compressionAlgorithm.decompress(compressedBytes, blockMetaData.uncompressedDataSize)

            val commandsArray = arrayOfNulls<Map.Entry<KeyAndTSN, Command>>(blockMetaData.commandCount)
            val decompressedBuffer = BytesBuffer(decompressedData)

            for (i in 0..commandsArray.lastIndex) {
                try {
                    val command = Command.readFromBytesBuffer(decompressedBuffer)
                        ?: throw IllegalStateException(
                            "Could not read command at index ${i} from the decompressed buffer -" +
                                " expected ${blockMetaData.commandCount} commands to be in the buffer!"
                        )
                    commandsArray[i] = ImmutableEntry(command.keyAndTSN, command)
                } catch (e: Exception) {
                    throw IllegalStateException(
                        "Could not read command at index ${i} (of ${blockMetaData.commandCount - 1} total) from the decompressed buffer! Cause: ${e}",
                        e
                    )
                }
            }

            if (decompressedBuffer.remaining > 0) {
                throw IllegalStateException(
                    "Failed to read data block: read all ${blockMetaData.commandCount} commands," +
                        " but there are ${decompressedBuffer.remaining} bytes left in the input buffer!"
                )
            }

            // this cast is 100% safe!
            // We collect the data in an array of commands which gets initialized with NULL values. Once we've extracted
            // all commands from the block, we will have filled every spot in the array with a non-NULL command.
            val commands = TreeMapUtils.treeMapFromSortedList(commandsArray.asList() as List<Map.Entry<KeyAndTSN, Command>>)

            return DataBlock(
                metaData = blockMetaData,
                data = commands,
            )

        }

    }


    private class ImmutableEntry<K, V>(
        override val key: K,
        override val value: V,
    ) : Map.Entry<K, V>

    /**
     * Returns the size of this block (metadata + data) in bytes.
     */
    val byteSize: Long
        get() {
            // add 10% overhead to the data size for the navigable map data structure
            return (this.metaData.uncompressedDataSize * 1.10).toLong() + metaData.byteSize
        }

    /**
     * Checks if there is any data in this block.
     *
     * @return `true` if the block is empty, otherwise `false`.
     */
    fun isEmpty(): Boolean {
        return this.data.isEmpty()
    }

    /**
     * Attempts to get the value for the given [key] from this block.
     *
     * @param key The key-and-TSN to get from the block.
     *
     * @return Either the successful result, or `null` if the key wasn't found at all.
     *         If the key was found, a pair is returned. The command is the command
     *         associated with the largest key in the block which is less than or equal
     *         to the requested key. The value is a boolean which indicates if the
     *         returned key is the **last** key in the block. If this happens to be
     *         the case, the next data block needs to be consulted as well, because
     *         the entry we returned may not be the **latest** entry for the key.
     */
    fun get(key: KeyAndTSN): Pair<Command, Boolean>? {
        if (key.key !in metaData.minKey..metaData.maxKey) {
            return null
        }
        if (key.tsn < metaData.minTSN) {
            return null
        }
        val (foundKeyAndTSN, foundCommand) = data.floorEntry(key)
            ?: return null // request key is too small

        // did we hit the same key?
        return if (foundKeyAndTSN.key == key.key) {
            // key is the same -> this is the entry we're looking for.
            Pair(foundCommand, data.lastKey() == foundKeyAndTSN)
        } else {
            // key is different -> the key we wanted doesn't exist.
            null
        }
    }

    inline fun <T> withCursor(action: (Cursor<KeyAndTSN, Command>) -> T): T {
        return cursor().use(action)
    }

    /**
     * Opens a new cursor on this block.
     *
     * The cursor needs to be closed explicitly.
     *
     * @return the new cursor.
     */
    fun cursor(): Cursor<KeyAndTSN, Command> {
        return if (this.data.isEmpty()) {
            EmptyCursor(
                getCursorName = { "Data Block #${this.metaData.blockSequenceNumber}" }
            )
        } else {
            NavigableMapCursor(this.data)
        }
    }

}