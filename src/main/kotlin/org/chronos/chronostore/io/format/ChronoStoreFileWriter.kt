package org.chronos.chronostore.io.format

import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnels
import org.chronos.chronostore.command.Command
import org.chronos.chronostore.command.KeyAndTimestamp
import org.chronos.chronostore.util.BloomFilterExtensions.toBytes
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.Bytes.Companion.put
import org.chronos.chronostore.util.Bytes.Companion.write
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianLong
import org.chronos.chronostore.util.PositionTrackingStream
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.sequence.OrderCheckingSequence.Companion.checkOrdered
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.lang.UnsupportedOperationException
import java.util.*

/**
 * Streaming writer for a *.chronostore file.
 */
class ChronoStoreFileWriter : AutoCloseable {

    private var closed = false

    private val outputStream: PositionTrackingStream

    private val settings: ChronoStoreFileSettings

    private val metadata: Map<Bytes, Bytes>

    companion object {

        /** The format version of the file. */
        val FILE_FORMAT_VERSION: ChronoStoreFileFormat.Version = ChronoStoreFileFormat.Version.V_1_0_0

    }

    /**
     *
     * @param outputStream The output stream to write to. Will be closed when the writer is closed.
     * @param settings The settings to use for writing the file. Will also be persisted in the file itself.
     */
    constructor(outputStream: OutputStream, settings: ChronoStoreFileSettings, metadata: Map<Bytes, Bytes>) {
        this.outputStream = PositionTrackingStream(outputStream.buffered())
        this.settings = settings
        this.metadata = metadata
    }

    /**
     * Writes the given [orderedCommands] (as well as any required additional information) to the [outputStream].
     *
     * @param orderedCommands The sequence of commands to write. **MUST** be ordered!
     */
    fun writeFile(orderedCommands: Sequence<Command>) {
        // grab the system clock time (will be written into the file later on)
        val wallClockTime = System.currentTimeMillis()

        // the file starts with the magic bytes (for later filetype recognition; fixed size)
        this.outputStream.write(ChronoStoreFileFormat.FILE_MAGIC_BYTES)
        // the next 4 bytes are reserved for the file format version (fixed size)
        this.outputStream.writeLittleEndianInt(FILE_FORMAT_VERSION.versionInt)

        val beginOfBlocks = this.outputStream.position

        val blockWriteResult = this.writeBlocks(orderedCommands)

        val beginOfIndexOfBlocks = this.outputStream.position
        // write the index of blocks
        for ((blockIndex, startPosition, minKeyAndTimestamp) in blockWriteResult.indexOfBlocks) {
            this.outputStream.writeLittleEndianInt(blockIndex)
            this.outputStream.writeLittleEndianLong(startPosition)
            minKeyAndTimestamp.writeTo(this.outputStream)
        }

        val beginOfMetadata = this.outputStream.position

        val metadata = FileMetaData(
            settings = this.settings,
            fileUUID = UUID.randomUUID(),
            minTimestamp = blockWriteResult.minTimestamp,
            maxTimestamp = blockWriteResult.maxTimestamp,
            minKey = blockWriteResult.minKey,
            maxKey = blockWriteResult.maxKey,
            headEntries = blockWriteResult.headEntries,
            totalEntries = blockWriteResult.totalEntries,
            createdAt = wallClockTime,
            infoMap = this.metadata,
        )

        metadata.writeTo(this.outputStream)

        val trailer = FileTrailer(
            beginOfBlocks = beginOfBlocks,
            beginOfIndexOfBlocks = beginOfIndexOfBlocks,
            beginOfMetadata = beginOfMetadata
        )
        trailer.writeTo(outputStream)

        this.outputStream.flush()
    }

    private fun writeBlocks(orderedCommands: Sequence<Command>): BlockWriteResult {
        // ensure that the commands we receive really are ordered,
        // as a command which appears out of order would be fatal.
        // Note that "checkOrdered" doesn't "sort" the input, it
        // just lazily checks that each element is greater than
        // the previous one. We also demand "strictly greater",
        // i.e. we do not allow the same command to exist multiple times.
        val commandsIterator = Iterators.peekingIterator(
            orderedCommands.checkOrdered(strict = true).iterator()
        )

        var totalEntries = 0L
        var headEntries = 0L

        var minTimestamp = Timestamp.MAX_VALUE
        var maxTimestamp = 0L

        var minKey: Bytes? = null
        var maxKey: Bytes? = null

        val commands = ObservingPeekingIterator(commandsIterator) { command ->
            // each command contributes towards the total entries
            totalEntries += 1
            minTimestamp = min(command.timestamp, minTimestamp)
            maxTimestamp = max(command.timestamp, maxTimestamp)
            minKey = min(minKey ?: command.key, command.key)
            maxKey = max(maxKey ?: command.key, command.key)

            if (!commandsIterator.hasNext() || commandsIterator.peek().key != command.key) {
                // the entry has no successor on the same key
                if (command.opCode != Command.OpCode.DEL) {
                    // the entry belongs to the HEAD revision of the file
                    headEntries++
                }
            }
        }

        // write the individual blocks into the file until we
        // run out of commands.
        val blockIndexToStartPositionAndMinKey = mutableListOf<Triple<Int, Long, KeyAndTimestamp>>()

        var blockSequenceNumber = 0
        while (commands.hasNext()) {
            val minKeyAndTimestamp = commands.peek().keyAndTimestamp
            blockIndexToStartPositionAndMinKey += Triple(blockSequenceNumber, this.outputStream.position, minKeyAndTimestamp)
            writeBlock(commands, blockSequenceNumber)
            blockSequenceNumber++
        }

        return BlockWriteResult(
            totalEntries = totalEntries,
            headEntries = headEntries,
            minTimestamp = minTimestamp.takeIf { it < Timestamp.MAX_VALUE },
            maxTimestamp = maxTimestamp.takeIf { it > 0 },
            minKey = minKey,
            maxKey = maxKey,
            indexOfBlocks = blockIndexToStartPositionAndMinKey
        )
    }

    private fun writeBlock(commands: PeekingIterator<Command>, blockSequenceNumber: Int) {
        if (!commands.hasNext()) {
            // no commands -> no need to start a block.
            return
        }
        // write the (raw) bytes of the block to a separate, intermediate array first.
        val blockDataOutputStream = ByteArrayOutputStream()
        val blockPositionTrackingStream = PositionTrackingStream(blockDataOutputStream)

        // The block index is a list of pairs:
        // - each key contains the key and the timestamp of a command in the file.
        // - each value contains the offset of that key (in bytes, offset 0 is start of block)
        val blockIndexBuilder = BlockIndexBuilder(this.settings.indexRate)

        // for the block metadata, we also need to keep track of the number of commands
        // in the block as well as the minimum and maximum timestamps we've encountered.
        var commandCount = 0
        var minTimestamp = Timestamp.MAX_VALUE
        var maxTimestamp = 0L
        // in order to create an appropriately sized bloom filter for the block later on,
        // we must keep track of all of the keys within the block.
        val allKeysInBlock = mutableListOf<Bytes>()
        while (commands.hasNext() && (blockPositionTrackingStream.position + commands.peek().byteSize) < this.settings.maxBlockSizeInBytes) {
            val command = commands.next()
            blockIndexBuilder.visit(command, blockPositionTrackingStream.position.toInt())
            blockPositionTrackingStream.write(command.toBytes())
            minTimestamp = min(minTimestamp, command.timestamp)
            maxTimestamp = max(maxTimestamp, command.timestamp)
            allKeysInBlock += command.keyAndTimestamp.key
            commandCount++
        }
        // we're done writing the commands. Complete the index construction
        val blockIndex = blockIndexBuilder.build()

        // these are the uncompressed bytes of the block.
        blockPositionTrackingStream.flush()
        val blockDataArrayRaw = blockDataOutputStream.toByteArray()
        // compress the data
        val blockDataArrayCompressed = this.settings.compression.compress(blockDataArrayRaw)
        // record the sizes
        val uncompressedSize = blockDataArrayRaw.size
        val compressedSize = blockDataArrayCompressed.size

        // prepare the bloom filter
        val bloomFilterBytes = createBlockBloomFilter(allKeysInBlock)

        // create the block metadata
        val blockMetaData = BlockMetaData(
            blockSequenceNumber = blockSequenceNumber,
            minKey = blockIndex.first().first.key,
            maxKey = blockIndex.last().first.key,
            minTimestamp = minTimestamp,
            maxTimestamp = maxTimestamp,
            commandCount = commandCount,
            compressedDataSize = compressedSize,
            uncompressedDataSize = uncompressedSize,
        )

        // serialize the index
        val blockIndexBytes = convertBlockIndexToBytes(blockIndex)

        // write the "magic bytes" that demarcate the beginning of a block
        this.outputStream.write(ChronoStoreFileFormat.BLOCK_MAGIC_BYTES)
        // write the total size of the block
        this.outputStream.writeLittleEndianInt(
            computeTotalBlockSize(
                blockMetaDataSize = blockMetaData.byteSize,
                bloomFilterBytes = bloomFilterBytes.size,
                blockIndexBytesSize = blockIndexBytes.size,
                blockDataArrayCompressedSize = blockDataArrayCompressed.size
            )
        )
        // write the size of the block metadata
        this.outputStream.writeLittleEndianInt(blockMetaData.byteSize)
        // write the metadata of the block
        blockMetaData.writeTo(this.outputStream)
        // write the size of the bloom filter
        this.outputStream.writeLittleEndianInt(bloomFilterBytes.size)
        // write the content of the bloom filter
        this.outputStream.write(bloomFilterBytes)
        // write the size of the block index
        outputStream.writeLittleEndianInt(blockIndexBytes.size)
        // write the index of the block
        outputStream.write(blockIndexBytes)
        // write the compressed size of the block
        this.outputStream.writeLittleEndianInt(compressedSize)
        // write the compressed block bytes
        this.outputStream.write(blockDataArrayCompressed)
    }

    @Suppress("UnstableApiUsage")
    private fun createBlockBloomFilter(allKeysInBlock: List<Bytes>): Bytes {
        // create a bloom filter to track which elements *might* be in the block
        val bloomFilter = BloomFilter.create(
            /* funnel = */ Funnels.byteArrayFunnel(),
            /* expectedInsertions = */ allKeysInBlock.size,
            /* fpp = */ 0.01
        )

        // add all entries to the bloom filter
        for (key in allKeysInBlock) {
            bloomFilter.put(key)
        }

        return bloomFilter.toBytes()
    }

    private fun computeTotalBlockSize(blockMetaDataSize: Int, bloomFilterBytes: Int, blockIndexBytesSize: Int, blockDataArrayCompressedSize: Int): Int {
        return Int.SIZE_BYTES + // size of metadata
            blockMetaDataSize + // metadata content bytes
            Int.SIZE_BYTES + // size of the bloom filter
            bloomFilterBytes + // content of the bloom filter
            Int.SIZE_BYTES + // size of block index
            blockIndexBytesSize + // block index content
            Int.SIZE_BYTES + // compressed size of the block
            blockDataArrayCompressedSize  // block content
    }

    private fun convertBlockIndexToBytes(blockIndex: List<Pair<KeyAndTimestamp, Int>>): Bytes {
        val outputStream = ByteArrayOutputStream()
        for ((keyAndTimestamp, offset) in blockIndex) {
            keyAndTimestamp.writeTo(outputStream)
            outputStream.writeLittleEndianInt(offset)
        }
        return Bytes(outputStream.toByteArray())
    }

    override fun close() {
        if (closed) {
            return
        }
        this.closed = true
        this.outputStream.close()
    }

    private fun <T : Comparable<T>> min(left: T, right: T): T {
        return if (left < right) {
            left
        } else {
            right
        }
    }

    private fun <T : Comparable<T>> max(left: T, right: T): T {
        return if (left > right) {
            left
        } else {
            right
        }
    }

    private class BlockWriteResult(
        val totalEntries: Long,
        val headEntries: Long,
        val minKey: Bytes?,
        val maxKey: Bytes?,
        val minTimestamp: Timestamp?,
        val maxTimestamp: Timestamp?,
        val indexOfBlocks: List<Triple<Int, Long, KeyAndTimestamp>>,
    )

    private class ObservingPeekingIterator<E>(
        private val peekingIterator: PeekingIterator<E>,
        private val observer: (E) -> Unit,
    ) : PeekingIterator<E> {

        override fun remove() {
            throw UnsupportedOperationException()
        }

        override fun hasNext(): Boolean {
            return this.peekingIterator.hasNext()
        }

        override fun next(): E {
            val next = this.peekingIterator.next()
            observer(next)
            return next
        }

        override fun peek(): E {
            return this.peekingIterator.peek()
        }

    }
}