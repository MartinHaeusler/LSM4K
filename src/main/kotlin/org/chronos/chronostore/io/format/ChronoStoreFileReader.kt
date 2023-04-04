package org.chronos.chronostore.io.format

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import org.chronos.chronostore.command.Command
import org.chronos.chronostore.command.KeyAndTimestamp
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver
import java.util.*

class ChronoStoreFileReader : AutoCloseable {

    private val driver: RandomFileAccessDriver

    private val fileHeader: FileHeader
    private val blockCache: Cache<Int, Optional<DataBlock>>

    constructor(driver: RandomFileAccessDriver) {
        this.driver = driver
        this.fileHeader = loadFileHeader()
        this.blockCache = CacheBuilder.newBuilder().maximumSize(100).build()
    }

    /**
     * Internal constructor for copying a reader.
     *
     * The initialization phase of a ChronoStore file comes with some
     * overhead, which is skipped here because the file is immutable
     * and the reader we were copied from already did all the work for us.
     */
    private constructor(driver: RandomFileAccessDriver, header: FileHeader, blockCache: Cache<Int, Optional<DataBlock>>) {
        // skip all the validation, it has already been done for us.
        this.driver = driver
        this.fileHeader = header
        this.blockCache = blockCache
    }

    private fun loadFileHeader(): FileHeader {
        // read and validate the magic bytes
        val magicBytesAndVersion = driver.readBytes(0, ChronoStoreFileFormat.FILE_MAGIC_BYTES.size + Int.SIZE_BYTES)
        val magicBytes = magicBytesAndVersion.slice(0 until ChronoStoreFileFormat.FILE_MAGIC_BYTES.size)

        if (magicBytes != ChronoStoreFileFormat.FILE_MAGIC_BYTES) {
            throw IllegalStateException("The file '${driver.filePath}' has an unknown file format.")
        }
        val versionInt = magicBytesAndVersion.readLittleEndianInt(ChronoStoreFileFormat.FILE_MAGIC_BYTES.size)
        val fileFormatVersion = ChronoStoreFileFormat.Version.fromInt(versionInt)
        return fileFormatVersion.readFileHeader(this.driver)
    }


    fun get(keyAndTimestamp: KeyAndTimestamp): Command? {
        if (!this.fileHeader.metaData.mayContainKey(keyAndTimestamp.key)) {
            // key is definitely not in this file, no point in searching.
            return null
        }
        if (!this.fileHeader.metaData.mayContainDataRelevantForTimestamp(keyAndTimestamp.timestamp)) {
            // the data in this file is too new and doesn't contain anything relevant for the request timestamp.
            return null
        }
        // the key may be contained, let's check.
        var blockIndex = this.fileHeader.indexOfBlocks.getBlockIndexForKeyAndTimestamp(keyAndTimestamp)
            ?: return null // we don't have a block for this key and timestamp.
        var matchingCommandFromPreviousBlock: Command? = null
        while (true) {
            val dataBlock = this.getBlockForIndex(blockIndex)
                ?: return matchingCommandFromPreviousBlock
            val (command, isLastInBlock) = dataBlock.get(keyAndTimestamp)
                ?: return matchingCommandFromPreviousBlock
            if (!isLastInBlock) {
                return command
            }
            // we've hit the last key in the block, so we need to consult the next block
            // in order to see if we find a newer entry which matches the key-and-timestamp.
            matchingCommandFromPreviousBlock = command
            // check the next block
            blockIndex++
        }
    }

    private fun getBlockForIndex(blockIndex: Int): DataBlock? {
        val cachedResult = this.blockCache.get(blockIndex) {
            val (startPosition, length) = this.fileHeader.indexOfBlocks.getBlockStartPositionAndLengthOrNull(blockIndex)
                ?: return@get Optional.empty()
            val blockBytes = this.driver.readBytes(startPosition, length)
            return@get Optional.of(DataBlock.readLazy(blockBytes.createInputStream(), this.fileHeader.metaData.settings.compression))
        }
        return cachedResult.orElse(null)
    }

    fun scan(from: KeyAndTimestamp?, fromInclusive: Boolean, to: KeyAndTimestamp?, toInclusive: Boolean, consumer: ScanClient) {
        return if (to == null) {
            scan(from, fromInclusive, consumer)
        } else {
            scan(from, fromInclusive, ScanUntilUpperLimitClient(consumer, to, toInclusive))
        }
    }

    fun scan(from: KeyAndTimestamp?, fromInclusive: Boolean, consumer: ScanClient) {
        TODO("Implement me!")
    }

    // TODO: maybe cursor API instead of just scans?

    /**
     * Creates a copy of this reader.
     *
     * Readers do not allow concurrent access because the drivers
     * are stateful (e.g. "seek" positions, OS constraints, etc).
     * In order to still allow multiple reader threads to access
     * the same file, readers can be copied.
     *
     * The copied reader will have no relationship with `this`
     * reader (except that both read the same file). It will need
     * to be managed and [closed][close] separately.
     *
     * @return A copy of this reader, for use by another thread.
     */
    fun copy(): ChronoStoreFileReader {
        return ChronoStoreFileReader(
            driver = this.driver.copy(),
            header = this.fileHeader,
            blockCache = this.blockCache,
        )
    }

    override fun close() {
        this.driver.close()
    }

    enum class ScanControl {

        CONTINUE,

        STOP;

    }

    fun interface ScanClient {

        fun inspect(command: Command): ScanControl

    }

    private class ScanUntilUpperLimitClient(
        private val consumer: ScanClient,
        private val upperBound: KeyAndTimestamp,
        private val upperBoundInclusive: Boolean,
    ) : ScanClient {

        override fun inspect(command: Command): ScanControl {
            val cmp = command.keyAndTimestamp.compareTo(this.upperBound)
            return when {
                cmp < 0 -> {
                    // upper bound hasn't been reached, keep going
                    this.consumer.inspect(command)
                }

                cmp > 0 -> {
                    // upper bound has been exceeded
                    ScanControl.STOP
                }

                else -> {
                    // we're AT the upper bound, now it depends if we want to see it
                    if (upperBoundInclusive) {
                        this.consumer.inspect(command)
                    } else {
                        ScanControl.STOP
                    }
                }
            }
        }

    }

}