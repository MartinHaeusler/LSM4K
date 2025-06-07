package io.github.martinhaeusler.lsm4k.io.format.writer

import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import io.github.martinhaeusler.lsm4k.io.format.*
import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.model.command.KeyAndTSN
import io.github.martinhaeusler.lsm4k.model.command.OpCode
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.writeLittleEndianInt
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.writeLittleEndianLong
import io.github.martinhaeusler.lsm4k.util.PositionTrackingStream
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.util.bloom.BytesBloomFilter
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes.Companion.writeBytesWithoutSize
import io.github.martinhaeusler.lsm4k.util.iterator.IteratorExtensions.checkOrdered
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsReporter
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.util.*

/**
 * Streaming writer for a *.lsm file.
 */
class StandardLSMStoreFileWriter : LSMStoreFileWriter {

    private var closed = false

    private val outputStream: PositionTrackingStream

    private val settings: LSMFileSettings

    private val statisticsReporter: StatisticsReporter

    companion object {

        /** The format version of the file. */
        val FILE_FORMAT_VERSION: FileFormatVersion = FileFormatVersion.V_1_0_0

        /** We allow 1% of false positives in the bloom filters. */
        private const val BLOOM_FILTER_FALSE_POSITIVE_PROBABILITY = 0.01
    }

    /**
     *
     * @param outputStream The output stream to write to. Will be closed when the writer is closed.
     * @param settings The settings to use for writing the file. Will also be persisted in the file itself.
     */
    @Suppress("ConvertSecondaryConstructorToPrimary")
    constructor(outputStream: OutputStream, settings: LSMFileSettings, statisticsReporter: StatisticsReporter) {
        this.outputStream = PositionTrackingStream(outputStream.buffered())
        this.settings = settings
        this.statisticsReporter = statisticsReporter
    }

    override fun write(
        numberOfMerges: Long,
        orderedCommands: Iterator<Command>,
        commandCountEstimate: Long,
        maxCompletelyWrittenTSN: TSN?,
    ) {
        if (orderedCommands.hasNext() && commandCountEstimate <= 0) {
            throw IllegalArgumentException(
                "Precondition violation - argument 'commandCountEstimate' (${commandCountEstimate})" +
                    " is not positive, but the command iterator has entries!"
            )
        }

        // grab the system clock time (will be written into the file later on)
        val wallClockTime = System.currentTimeMillis()

        // the file starts with the magic bytes (for later filetype recognition; fixed size)
        this.outputStream.writeBytesWithoutSize(LSMFileFormat.FILE_MAGIC_BYTES)

        // the next 4 bytes are reserved for the file format version (fixed size)
        this.outputStream.writeLittleEndianInt(FILE_FORMAT_VERSION.versionInt)

        val beginOfBlocks = this.outputStream.position

        val bloomFilter = BytesBloomFilter(
            expectedEntries = commandCountEstimate,
            falsePositiveRate = BLOOM_FILTER_FALSE_POSITIVE_PROBABILITY,
        )

        val blockWriteResult = this.writeBlocks(orderedCommands, bloomFilter)

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
            minTSN = blockWriteResult.minTSN,
            maxTSN = blockWriteResult.maxTSN,
            firstKeyAndTSN = blockWriteResult.firstKeyAndTSN,
            lastKeyAndTSN = blockWriteResult.lastKeyAndTSN,
            maxCompletelyWrittenTSN = maxCompletelyWrittenTSN,
            minKey = blockWriteResult.minKey,
            maxKey = blockWriteResult.maxKey,
            headEntries = blockWriteResult.headEntries,
            numberOfMerges = numberOfMerges,
            totalEntries = blockWriteResult.totalEntries,
            numberOfBlocks = blockWriteResult.numberOfBlocks,
            createdAt = wallClockTime,
            bloomFilter = bloomFilter,
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

    private fun writeBlocks(
        orderedCommands: Iterator<Command>,
        bloomFilter: BytesBloomFilter,
    ): BlockWriteResult {
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

        var minTSN = TSN.MAX_VALUE
        var maxTSN = 0L

        var minKey: Bytes? = null
        var maxKey: Bytes? = null

        var firstKeyAndTSN: KeyAndTSN? = null
        var lastKeyAndTSN: KeyAndTSN? = null

        val commands = ObservingPeekingIterator(commandsIterator) { command ->
            // each command contributes towards the total entries
            totalEntries += 1
            minTSN = min(command.tsn, minTSN)
            maxTSN = max(command.tsn, maxTSN)
            minKey = min(minKey ?: command.key, command.key)
            maxKey = max(maxKey ?: command.key, command.key)

            if (firstKeyAndTSN == null) {
                firstKeyAndTSN = command.keyAndTSN
            }
            lastKeyAndTSN = command.keyAndTSN

            if (!commandsIterator.hasNext() || commandsIterator.peek().key != command.key) {
                // the entry has no successor on the same key
                if (command.opCode != OpCode.DEL) {
                    // the entry belongs to the HEAD revision of the file
                    headEntries++
                }
            }
        }

        // write the individual blocks into the file until we
        // run out of commands.
        val blockIndexToStartPositionAndMinKey = mutableListOf<Triple<Int, Long, KeyAndTSN>>()

        var blockSequenceNumber = 0
        while (commands.hasNext()) {
            val nextCommand = commands.peek()
            val minKeyAndTSN = KeyAndTSN(
                // It is ESSENTIAL That we call ".own()" on the key here.
                // If we don't do that, we keep the whole backing array
                // of EVERY page in memory. This will eventually lead to
                // out-of-memory issues if the file we're writing has
                // enough blocks in it.
                nextCommand.key.own(),
                nextCommand.tsn
            )
            blockIndexToStartPositionAndMinKey += Triple(
                blockSequenceNumber,
                this.outputStream.position,
                minKeyAndTSN
            )

            // for debugging purposes, we write the block into a byte array first
            val blockBytes = ByteArrayOutputStream().use { baos ->
                writeBlock(commands, bloomFilter, blockSequenceNumber, baos)
                baos.flush()
                baos.toByteArray()
            }

            this.outputStream.write(blockBytes)

            // writeBlock(commands, blockSequenceNumber, this.outputStream)
            blockSequenceNumber++
        }

        return BlockWriteResult(
            totalEntries = totalEntries,
            headEntries = headEntries,
            minTSN = minTSN.takeIf { it < TSN.MAX_VALUE },
            maxTSN = maxTSN.takeIf { it > 0 },
            minKey = minKey,
            maxKey = maxKey,
            firstKeyAndTSN = firstKeyAndTSN,
            lastKeyAndTSN = lastKeyAndTSN,
            numberOfBlocks = blockSequenceNumber,
            indexOfBlocks = blockIndexToStartPositionAndMinKey
        )
    }

    private fun writeBlock(
        commands: PeekingIterator<Command>,
        bloomFilter: BytesBloomFilter,
        blockSequenceNumber: Int,
        outputStream: OutputStream,
    ) {
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
        var minTSN = TSN.MAX_VALUE
        var maxTSN = 0L

        // in order to create an appropriately sized bloom filter for the block later on,
        // we must keep track of all keys within the block.
        var firstKey: Bytes? = null
        var lastKey: Bytes? = null
        // fill the block with commands:
        // While we have more commands (and the block didn't get too big yet), write more commands to the block.
        while (commands.hasNext()) {
            // determine what the block size would be if we add the next entry to it.
            val blockSizeIncludingNextEntry = blockPositionTrackingStream.position + commands.peek().byteSize
            // we have a curious issue here: if *one* single entry is bigger than the block size, we
            // must allow it to go somewhere. To account for this, we say: if the current block is empty,
            // we allow the first entry to be of arbitrary size (even larger than the block size). If
            // the current block is *not* empty, we demand that the next entry still fits into the block;
            // if it doesn't, we close the current block and let the data flow into the next block instead.
            if (firstKey != null && blockSizeIncludingNextEntry >= this.settings.maxBlockSize.bytes) {
                // this block is full, don't continue writing.
                break
            }

            val command = commands.next()
            command.writeToStream(blockPositionTrackingStream)
            bloomFilter.put(command.key)

            minTSN = min(minTSN, command.tsn)
            maxTSN = max(maxTSN, command.tsn)

            val currentKey = command.keyAndTSN.key
            firstKey = firstKey ?: currentKey
            lastKey = currentKey
            commandCount++
        }

        // these are the uncompressed bytes of the block.
        blockPositionTrackingStream.flush()
        blockPositionTrackingStream.close()
        blockDataOutputStream.flush()
        blockDataOutputStream.close()
        val blockDataArrayRaw = blockDataOutputStream.toByteArray()
        // compress the data
        val blockDataArrayCompressed = this.settings.compression.compress(
            bytes = blockDataArrayRaw,
            statisticsReporter = this.statisticsReporter,
        )

        // record the sizes
        val uncompressedSize = blockDataArrayRaw.size
        val compressedSize = blockDataArrayCompressed.size

        // create the block metadata
        val blockMetaData = BlockMetaData(
            blockSequenceNumber = blockSequenceNumber,
            minKey = firstKey!!, // we know for a fact that we will have a first key
            maxKey = lastKey!!, // and a last key at this point because the iterator is non-empty.
            minTSN = minTSN,
            maxTSN = maxTSN,
            commandCount = commandCount,
            compressedDataSize = compressedSize,
            uncompressedDataSize = uncompressedSize,
        )

        // write the "magic bytes" that demarcate the beginning of a block
        outputStream.writeBytesWithoutSize(LSMFileFormat.BLOCK_MAGIC_BYTES)
        // write the total size of the block
        val computedTotalBlockSize = computeTotalBlockSize(
            blockMetaDataSize = blockMetaData.byteSize,
            blockDataArrayCompressedSize = blockDataArrayCompressed.size
        )
        outputStream.writeLittleEndianInt(computedTotalBlockSize)
        // write the size of the block metadata
        outputStream.writeLittleEndianInt(blockMetaData.byteSize)
        // write the metadata of the block
        blockMetaData.writeTo(outputStream)
        // write the compressed size of the block
        outputStream.writeLittleEndianInt(compressedSize)
        // write the compressed block bytes
        outputStream.write(blockDataArrayCompressed)
    }

    private fun computeTotalBlockSize(blockMetaDataSize: Int, blockDataArrayCompressedSize: Int): Int {
        return Int.SIZE_BYTES + // size of metadata
            blockMetaDataSize + // metadata content bytes
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
        val minTSN: TSN?,
        val maxTSN: TSN?,
        val firstKeyAndTSN: KeyAndTSN?,
        val lastKeyAndTSN: KeyAndTSN?,
        val numberOfBlocks: Int,
        val indexOfBlocks: List<Triple<Int, Long, KeyAndTSN>>,
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