package org.chronos.chronostore.io.format

import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnels
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.util.BloomFilterExtensions.toBytes
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.bytes.Bytes.Companion.put
import org.chronos.chronostore.util.bytes.Bytes.Companion.writeBytesWithoutSize
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianLong
import org.chronos.chronostore.util.PositionTrackingStream
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.iterator.IteratorExtensions.checkOrdered
import java.io.ByteArrayOutputStream
import java.io.OutputStream
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
    @Suppress("ConvertSecondaryConstructorToPrimary")
    constructor(outputStream: OutputStream, settings: ChronoStoreFileSettings, metadata: Map<Bytes, Bytes>) {
        this.outputStream = PositionTrackingStream(outputStream.buffered())
        this.settings = settings
        this.metadata = metadata
    }

    /**
     * Writes the given [orderedCommands] (as well as any required additional information) to the [outputStream].
     *
     * @param numberOfMerges The number of merges to record in the file metadata.
     * @param orderedCommands The sequence of commands to write. **MUST** be ordered!
     */
    fun writeFile(numberOfMerges: Long, orderedCommands: Iterator<Command>) {
        // grab the system clock time (will be written into the file later on)
        val wallClockTime = System.currentTimeMillis()

        // the file starts with the magic bytes (for later filetype recognition; fixed size)
        this.outputStream.writeBytesWithoutSize(ChronoStoreFileFormat.FILE_MAGIC_BYTES)
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
            numberOfMerges = numberOfMerges,
            totalEntries = blockWriteResult.totalEntries,
            numberOfBlocks = blockWriteResult.numberOfBlocks,
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

    private fun writeBlocks(orderedCommands: Iterator<Command>): BlockWriteResult {
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
            numberOfBlocks = blockSequenceNumber,
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

        // for the block metadata, we also need to keep track of the number of commands
        // in the block as well as the minimum and maximum timestamps we've encountered.
        var commandCount = 0
        var minTimestamp = Timestamp.MAX_VALUE
        var maxTimestamp = 0L
        // in order to create an appropriately sized bloom filter for the block later on,
        // we must keep track of all keys within the block.
        val allKeysInBlock = mutableListOf<Bytes>()
        while (commands.hasNext() && (blockPositionTrackingStream.position + commands.peek().byteSize) < this.settings.maxBlockSize.bytes) {
            val command = commands.next()
            command.writeToStream(blockPositionTrackingStream)
            minTimestamp = min(minTimestamp, command.timestamp)
            maxTimestamp = max(maxTimestamp, command.timestamp)
            allKeysInBlock += command.keyAndTimestamp.key
            commandCount++
        }

        // these are the uncompressed bytes of the block.
        blockPositionTrackingStream.flush()
        blockPositionTrackingStream.close()
        blockDataOutputStream.flush()
        blockDataOutputStream.close()
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
            minKey = allKeysInBlock.first(),
            maxKey = allKeysInBlock.last(),
            minTimestamp = minTimestamp,
            maxTimestamp = maxTimestamp,
            commandCount = commandCount,
            compressedDataSize = compressedSize,
            uncompressedDataSize = uncompressedSize,
        )

        // write the "magic bytes" that demarcate the beginning of a block
        this.outputStream.writeBytesWithoutSize(ChronoStoreFileFormat.BLOCK_MAGIC_BYTES)
        // write the total size of the block
        val computedTotalBlockSize = computeTotalBlockSize(
            blockMetaDataSize = blockMetaData.byteSize,
            bloomFilterBytes = bloomFilterBytes.size,
            blockDataArrayCompressedSize = blockDataArrayCompressed.size
        )
        this.outputStream.writeLittleEndianInt(computedTotalBlockSize)
        // write the size of the block metadata
        this.outputStream.writeLittleEndianInt(blockMetaData.byteSize)
        // write the metadata of the block
        blockMetaData.writeTo(this.outputStream)
        // write the size of the bloom filter
        this.outputStream.writeLittleEndianInt(bloomFilterBytes.size)
        // write the content of the bloom filter
        this.outputStream.writeBytesWithoutSize(bloomFilterBytes)
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

    private fun computeTotalBlockSize(blockMetaDataSize: Int, bloomFilterBytes: Int, blockDataArrayCompressedSize: Int): Int {
        return Int.SIZE_BYTES + // size of metadata
            blockMetaDataSize + // metadata content bytes
            Int.SIZE_BYTES + // size of the bloom filter
            bloomFilterBytes + // content of the bloom filter
            Int.SIZE_BYTES + // compressed size of the block
            blockDataArrayCompressedSize  // block content
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
        val numberOfBlocks: Int,
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