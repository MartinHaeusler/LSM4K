package org.chronos.chronostore.io.format

import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnels
import org.chronos.chronostore.command.Command
import org.chronos.chronostore.command.KeyAndTimestamp
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.Bytes.Companion.mightContain
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianInt
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.util.*


interface DataBlock {

    val metaData: BlockMetaData

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
    fun get(key: KeyAndTimestamp): Pair<Command, Boolean>?



    companion object {

        fun readEager(inputStream: InputStream, compressionAlgorithm: CompressionAlgorithm): DataBlock {
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

            return EagerDataBlock(blockMetaData, commands)
        }


        fun readLazy(inputStream: InputStream, compressionAlgorithm: CompressionAlgorithm): DataBlock {
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

            return LazyDataBlock(blockMetaData, bloomFilter, blockIndex, decompressedData)
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

class EagerDataBlock(
    override val metaData: BlockMetaData,
    private val data: NavigableMap<KeyAndTimestamp, Command>,
) : DataBlock {

    override fun get(key: KeyAndTimestamp): Pair<Command, Boolean>? {
        if (key.key !in metaData.minKey..metaData.maxKey) {
            return null
        }
        if (key.timestamp < metaData.minTimestamp) {
            return null
        }
        val (foundKeyAndTimestamp, foundCommand) = data.floorEntry(key)
            ?: return null // request key is too small

        // did we hit the same key?
        return if (foundKeyAndTimestamp.key == key.key) {
            // key is the same -> this is the entry we're looking for.
            Pair(foundCommand, data.lastKey() == foundKeyAndTimestamp)
        } else {
            // key is different -> the key we wanted doesn't exist.
            null
        }
    }

}

class LazyDataBlock(
    override val metaData: BlockMetaData,
    private val bloomFilter: BloomFilter<ByteArray>,
    private val blockIndex: NavigableMap<KeyAndTimestamp, Int>,
    private val data: Bytes,
) : DataBlock {

    override fun get(key: KeyAndTimestamp): Pair<Command, Boolean>? {
        if (key.key !in metaData.minKey..metaData.maxKey) {
            return null
        }
        if (key.timestamp < metaData.minTimestamp) {
            return null
        }
        if (!bloomFilter.mightContain(key.key)) {
            // key isn't in our block
            return null
        }
        val (_, foundOffset) = blockIndex.floorEntry(key)
            ?: return null // request key is too small

        data.createInputStream(foundOffset).use { inputStream ->
            var previous: Command? = null
            while (inputStream.available() > 0) {
                val command = Command.readFromStream(inputStream)
                val cmp = command.keyAndTimestamp.compareTo(key)
                if (cmp == 0) {
                    // exact match (unlikely, but possible)
                    return Pair(command, false)
                } else if (cmp > 0) {
                    // this key is bigger, use the previous one
                    return if (previous?.key == key.key) {
                        Pair(previous, false)
                    } else {
                        null
                    }
                } else {
                    // remember this one, but keep searching
                    previous = command
                }
            }
            // we're at the end of the block, maybe the last
            // entry matches?
            return if (previous?.key == key.key) {
                Pair(previous, true)
            } else {
                null
            }
        }

    }

}