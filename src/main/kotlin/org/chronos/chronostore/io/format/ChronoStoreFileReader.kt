package org.chronos.chronostore.io.format

import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver
import org.chronos.chronostore.io.format.datablock.BlockReadMode
import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.lsm.LocalBlockCache
import org.chronos.chronostore.util.cursor.Cursor

class ChronoStoreFileReader : AutoCloseable {

    companion object {

        @JvmStatic
        fun loadFileHeader(driver: RandomFileAccessDriver): FileHeader {
            // read and validate the magic bytes
            val magicBytesAndVersion = driver.readBytes(0, ChronoStoreFileFormat.FILE_MAGIC_BYTES.size + Int.SIZE_BYTES)
            val magicBytes = magicBytesAndVersion.slice(0 until ChronoStoreFileFormat.FILE_MAGIC_BYTES.size)

            if (magicBytes != ChronoStoreFileFormat.FILE_MAGIC_BYTES) {
                throw IllegalStateException("The file '${driver.filePath}' has an unknown file format.")
            }
            val versionInt = magicBytesAndVersion.readLittleEndianInt(ChronoStoreFileFormat.FILE_MAGIC_BYTES.size)
            val fileFormatVersion = ChronoStoreFileFormat.Version.fromInt(versionInt)
            return fileFormatVersion.readFileHeader(driver)
        }

    }


    private val driver: RandomFileAccessDriver
    private val blockReadMode: BlockReadMode

    val fileHeader: FileHeader
    private val blockCache: LocalBlockCache

    constructor(
        driver: RandomFileAccessDriver,
        blockReadMode: BlockReadMode,
        blockCache: LocalBlockCache
    ) {
        this.driver = driver
        this.blockReadMode = blockReadMode
        this.fileHeader = loadFileHeader(driver)
        this.blockCache = blockCache
    }

    constructor(
        driver: RandomFileAccessDriver,
        header: FileHeader,
        blockCache: LocalBlockCache,
        blockReadMode: BlockReadMode
    ) {
        // skip all the validation, it has already been done for us.
        this.driver = driver
        this.blockReadMode = blockReadMode
        this.fileHeader = header
        this.blockCache = blockCache
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
        var blockIndex = this.fileHeader.indexOfBlocks.getBlockIndexForKeyAndTimestampAscending(keyAndTimestamp)
            ?: return null // we don't have a block for this key and timestamp.
        var matchingCommandFromPreviousBlock: Command? = null
        while (true) {
            val dataBlock = this.getBlockForIndex(blockIndex)
                ?: return matchingCommandFromPreviousBlock
            val (command, isLastInBlock) = dataBlock.get(keyAndTimestamp, this.driver)
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
        return this.blockCache.getBlock(this.fileHeader.metaData.fileUUID, blockIndex, ::getBlockForIndexUncached)
    }

    private fun getBlockForIndexUncached(blockIndex: Int): DataBlock? {
        val (startPosition, length) = this.fileHeader.indexOfBlocks.getBlockStartPositionAndLengthOrNull(blockIndex)
            ?: return null
        val blockBytes = this.driver.readBytes(startPosition, length)
        val compressionAlgorithm = this.fileHeader.metaData.settings.compression
        return when (this.blockReadMode) {
            BlockReadMode.IN_MEMORY_EAGER -> DataBlock.createEagerLoadingInMemoryBlock(blockBytes.createInputStream(), compressionAlgorithm)
            BlockReadMode.IN_MEMORY_LAZY -> DataBlock.createLazyLoadingInMemoryBlock(blockBytes.createInputStream(), compressionAlgorithm)
            BlockReadMode.DISK_BASED -> DataBlock.createDiskBasedDataBlock(blockBytes.createInputStream(), compressionAlgorithm, startPosition)
        }
    }

    fun openCursor(): Cursor<KeyAndTimestamp, Command> {
        return ChronoStoreFileCursor()
    }

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
            blockReadMode = this.blockReadMode,
            header = this.fileHeader,
            blockCache = this.blockCache,
        )
    }

    override fun toString(): String {
        return "ChronoStoreFileReader[${this.driver.filePath}]"
    }

    override fun close() {
        this.driver.close()
    }

    private inner class ChronoStoreFileCursor : Cursor<KeyAndTimestamp, Command> {

        override var modCount: Long = 0

        override var isOpen: Boolean = true

        override var isValidPosition: Boolean = false

        private var currentBlock: DataBlock? = null
        private var currentCursor: Cursor<KeyAndTimestamp, Command>? = null

        private val driver: RandomFileAccessDriver
            get() = this@ChronoStoreFileReader.driver

        override fun invalidatePosition() {
            check(this.isOpen, this::getAlreadyClosedMessage)
            this.currentCursor?.close()
            this.currentCursor = null
            this.currentBlock = null
            this.isValidPosition = false
            this.modCount++
        }

        override fun first(): Boolean {
            check(this.isOpen, this::getAlreadyClosedMessage)
            this.invalidatePosition()
            val firstBlock = this@ChronoStoreFileReader.getBlockForIndex(0)
            if (firstBlock == null || firstBlock.isEmpty()) {
                this.isValidPosition = false
                return false
            }
            this.currentBlock = firstBlock
            val cursor = firstBlock.cursor(this.driver)
            this.currentCursor = cursor
            // the block isn't empty, so we have a first element.
            cursor.firstOrThrow()
            this.isValidPosition = true
            return true
        }

        override fun last(): Boolean {
            check(this.isOpen, this::getAlreadyClosedMessage)
            this.invalidatePosition()
            val numberOfBlocks = fileHeader.metaData.numberOfBlocks
            val lastBlock = getBlockForIndex(numberOfBlocks - 1)
            if (lastBlock == null || lastBlock.isEmpty()) {
                this.isValidPosition = false
                return false
            }
            this.currentBlock = lastBlock
            val cursor = lastBlock.cursor(this.driver)
            this.currentCursor = cursor
            // the block isn't empty, so we have a last element.
            cursor.lastOrThrow()
            this.isValidPosition = true
            return true
        }

        override fun next(): Boolean {
            check(this.isOpen, this::getAlreadyClosedMessage)
            if (!this.isValidPosition) {
                return false
            }
            val block = this.currentBlock
            val cursor = this.currentCursor
            if (block == null || cursor == null) {
                return false
            }
            if (cursor.next()) {
                this.modCount++
                return true
            }
            // open the cursor on the next block
            val nextBlockIndex = block.metaData.blockSequenceNumber + 1
            if (nextBlockIndex >= fileHeader.metaData.numberOfBlocks) {
                // there is no next position; keep the current cursor
                this.modCount++
                return false
            }
            val newBlock = getBlockForIndex(nextBlockIndex)
                ?: throw IllegalStateException("Could not get block with index ${nextBlockIndex} in file '${driver.filePath}'!")
            val newCursor = newBlock.cursor(this.driver)
            newCursor.firstOrThrow()
            this.currentBlock = newBlock
            this.currentCursor = newCursor
            return true
        }

        override fun previous(): Boolean {
            check(this.isOpen, this::getAlreadyClosedMessage)
            if (!this.isValidPosition) {
                return false
            }
            val block = this.currentBlock
            val cursor = this.currentCursor
            if (block == null || cursor == null) {
                return false
            }
            if (cursor.previous()) {
                this.modCount++
                return true
            }
            // open the cursor on the previous block
            val previousBlockIndex = block.metaData.blockSequenceNumber - 1
            if (previousBlockIndex < 0) {
                // there is no previous position; keep the current cursor
                this.modCount++
                return false
            }
            val newBlock = getBlockForIndex(previousBlockIndex)
                ?: throw IllegalStateException("Could not get block with index ${previousBlockIndex} in file '${driver.filePath}'!")
            val newCursor = newBlock.cursor(this.driver)
            newCursor.lastOrThrow()
            this.currentBlock = newBlock
            this.currentCursor = newCursor
            return true
        }

        override val keyOrNull: KeyAndTimestamp?
            get() {
                check(this.isOpen, this::getAlreadyClosedMessage)
                if (!this.isValidPosition) {
                    return null
                }
                return this.currentCursor?.keyOrNull
            }

        override val valueOrNull: Command?
            get() {
                check(this.isOpen, this::getAlreadyClosedMessage)
                if (!this.isValidPosition) {
                    return null
                }
                return this.currentCursor?.valueOrNull
            }

        override fun close() {
            if (!this.isOpen) {
                return
            }
            this.isOpen = false
            this.currentCursor?.close()
        }

        override fun seekExactlyOrNext(key: KeyAndTimestamp): Boolean {
            check(this.isOpen, this::getAlreadyClosedMessage)
            this.invalidatePosition()
            val blockIndex = fileHeader.indexOfBlocks.getBlockIndexForKeyAndTimestampAscending(key)
                ?: return false
            val block = getBlockForIndex(blockIndex)
                ?: return false
            val cursor = block.cursor(this.driver)

            if (!cursor.seekExactlyOrNext(key)) {
                cursor.close()
                return false
            }
            this.currentBlock = block
            this.currentCursor = cursor
            return true
        }

        override fun seekExactlyOrPrevious(key: KeyAndTimestamp): Boolean {
            check(this.isOpen, this::getAlreadyClosedMessage)
            this.invalidatePosition()
            val blockIndex = fileHeader.indexOfBlocks.getBlockIndexForKeyAndTimestampDescending(key)
                ?: return false
            val block = getBlockForIndex(blockIndex)
                ?: return false
            val cursor = block.cursor(this.driver)
            if (!cursor.seekExactlyOrPrevious(key)) {
                cursor.close()
                return false
            }
            this.currentBlock = block
            this.currentCursor = cursor
            return true
        }

        private fun getAlreadyClosedMessage(): String {
            return "This cursor on ${this.driver.filePath} has already been closed!"
        }
    }
}