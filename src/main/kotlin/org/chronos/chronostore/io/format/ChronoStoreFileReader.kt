package org.chronos.chronostore.io.format

import org.chronos.chronostore.api.exceptions.BlockReadException
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver
import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.lsm.cache.LocalBlockCache
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTSN
import org.chronos.chronostore.util.cursor.CloseHandler
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.CursorUtils
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics

class ChronoStoreFileReader : AutoCloseable {

    companion object {

        inline fun <T> RandomFileAccessDriver.withChronoStoreFileReader(
            blockCache: LocalBlockCache,
            action: (ChronoStoreFileReader) -> T,
        ): T {
            return ChronoStoreFileReader(
                driver = this,
                blockCache = blockCache,
            ).use(action)
        }

        inline fun <T> RandomFileAccessDriver.withChronoStoreFileReader(
            blockCache: LocalBlockCache,
            header: FileHeader,
            action: (ChronoStoreFileReader) -> T,
        ): T {
            return ChronoStoreFileReader(
                driver = this,
                header = header,
                blockCache = blockCache
            ).use(action)
        }


        @JvmStatic
        fun loadFileHeader(driver: RandomFileAccessDriver): FileHeader {
            // read and validate the magic bytes
            val magicBytesAndVersion = driver.readBytes(0, ChronoStoreFileFormat.FILE_MAGIC_BYTES.size + Int.SIZE_BYTES)
            val magicBytes = magicBytesAndVersion.slice(0, ChronoStoreFileFormat.FILE_MAGIC_BYTES.size)

            if (magicBytes != ChronoStoreFileFormat.FILE_MAGIC_BYTES) {
                throw IllegalStateException("The file '${driver.filePath}' has an unknown file format. Expected ${ChronoStoreFileFormat.FILE_MAGIC_BYTES.hex()} but got ${magicBytes.hex()}!")
            }
            val versionInt = magicBytesAndVersion.readLittleEndianInt(ChronoStoreFileFormat.FILE_MAGIC_BYTES.size)
            val fileFormatVersion = ChronoStoreFileFormat.Version.fromInt(versionInt)
            return fileFormatVersion.readFileHeader(driver)
        }

    }


    private val driver: RandomFileAccessDriver

    val fileHeader: FileHeader
    private val blockCache: LocalBlockCache

    constructor(
        driver: RandomFileAccessDriver,
        blockCache: LocalBlockCache,
    ) {
        this.driver = driver
        this.fileHeader = loadFileHeader(driver)
        this.blockCache = blockCache
    }

    constructor(
        driver: RandomFileAccessDriver,
        header: FileHeader,
        blockCache: LocalBlockCache,
    ) {
        // skip all the validation, it has already been done for us.
        this.driver = driver
        this.fileHeader = header
        this.blockCache = blockCache
    }

    fun getLatestVersion(keyAndTSN: KeyAndTSN): Command? {
        if (!this.fileHeader.metaData.mayContainKey(keyAndTSN.key)) {
            // key is definitely not in this file, no point in searching.
            return null
        }
        if (!this.fileHeader.metaData.mayContainDataRelevantForTSN(keyAndTSN.tsn)) {
            // the data in this file is too new and doesn't contain anything relevant for the request timestamp.
            return null
        }
        // the key may be contained, let's check.
        var blockIndex = this.fileHeader.indexOfBlocks.getBlockIndexForKeyAndTimestampDescending(keyAndTSN)
            ?: return null // we don't have a block for this key and timestamp.
        var matchingCommandFromPreviousBlock: Command? = null
        while (true) {
            val dataBlock = this.getBlockForIndex(blockIndex)
                ?: return matchingCommandFromPreviousBlock
            val (command, isLastInBlock) = dataBlock.get(keyAndTSN)
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
        if (blockIndex >= this.fileHeader.metaData.numberOfBlocks || blockIndex < 0) {
            // block index is out of range, no need to look for it.
            return null
        }
        try {
            return this.blockCache.getBlock(this.fileHeader.metaData.fileUUID, blockIndex, ::getBlockForIndexUncached)
        } catch (e: Exception) {
            throw BlockReadException("Failed to read block #${blockIndex} from file '${this.driver.filePath}'. \nCause: ${e}", e)
        }
    }

    private fun getBlockForIndexUncached(blockIndex: Int): DataBlock {
        val timeBefore = System.currentTimeMillis()
        val (startPosition, length) = this.fileHeader.indexOfBlocks.getBlockStartPositionAndLengthOrNull(blockIndex)
            ?: throw IllegalStateException(
                "Could not fetch block #${blockIndex} from Store '${this.blockCache.storeId}' (file: '${this.fileHeader.metaData.fileUUID}')!" +
                    " The block index contains ${this.fileHeader.indexOfBlocks.size} entries."
            )
        val blockBytes = this.driver.readBytes(startPosition, length)
        val compressionAlgorithm = this.fileHeader.metaData.settings.compression
        try {
            val dataBlock = DataBlock.loadBlock(blockBytes, compressionAlgorithm)
            val timeAfter = System.currentTimeMillis()
            ChronoStoreStatistics.BLOCK_LOAD_TIME.addAndGet(timeAfter - timeBefore)
            return dataBlock
        } catch (e: Exception) {
            throw BlockReadException(
                message = "Failed to read block #${blockIndex} of file '${this.driver.filePath}'." +
                    " This file is potentially corrupted! Cause: ${e}",
                cause = e
            )
        }
    }

    inline fun <T> withCursor(action: (Cursor<KeyAndTSN, Command>) -> T): T {
        return this.openCursor().use(action)
    }

    fun openCursor(): Cursor<KeyAndTSN, Command> {
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

    private inner class ChronoStoreFileCursor : Cursor<KeyAndTSN, Command> {

        override var modCount: Long = 0

        override var isOpen: Boolean = true

        override var isValidPosition: Boolean = false

        private var currentBlock: DataBlock? = null
        private var currentCursor: Cursor<KeyAndTSN, Command>? = null

        private val closeHandlers = mutableListOf<CloseHandler>()

        init {
            ChronoStoreStatistics.FILE_CURSORS.incrementAndGet()
        }

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
            ChronoStoreStatistics.FILE_CURSOR_FIRST_SEEKS.incrementAndGet()
            this.invalidatePosition()
            val firstBlock = this@ChronoStoreFileReader.getBlockForIndex(0)
            if (firstBlock == null || firstBlock.isEmpty()) {
                this.isValidPosition = false
                return false
            }
            this.currentBlock = firstBlock
            val cursor = firstBlock.cursor()
            this.currentCursor = cursor
            // the block isn't empty, so we have a first element.
            cursor.firstOrThrow()
            this.isValidPosition = true
            return true
        }

        override fun last(): Boolean {
            check(this.isOpen, this::getAlreadyClosedMessage)
            ChronoStoreStatistics.FILE_CURSOR_LAST_SEEKS.incrementAndGet()
            this.invalidatePosition()
            val numberOfBlocks = fileHeader.metaData.numberOfBlocks
            val lastBlock = getBlockForIndex(numberOfBlocks - 1)
            if (lastBlock == null || lastBlock.isEmpty()) {
                this.isValidPosition = false
                return false
            }
            this.currentBlock = lastBlock
            val cursor = lastBlock.cursor()
            this.currentCursor = cursor
            // the block isn't empty, so we have a last element.
            cursor.lastOrThrow()
            this.isValidPosition = true
            return true
        }

        override fun next(): Boolean {
            check(this.isOpen, this::getAlreadyClosedMessage)
            ChronoStoreStatistics.FILE_CURSOR_NEXT_SEEKS.incrementAndGet()
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
            val newCursor = newBlock.cursor()
            newCursor.firstOrThrow()
            this.currentBlock = newBlock
            this.currentCursor = newCursor
            return true
        }

        override fun previous(): Boolean {
            check(this.isOpen, this::getAlreadyClosedMessage)
            ChronoStoreStatistics.FILE_CURSOR_PREVIOUS_SEEKS.incrementAndGet()
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
            val newCursor = newBlock.cursor()
            newCursor.lastOrThrow()
            this.currentBlock = newBlock
            this.currentCursor = newCursor
            return true
        }

        override fun peekNext(): Pair<KeyAndTSN, Command>? {
            if (!this.isValidPosition) {
                return null
            }
            val cursor = this.currentCursor
                ?: return super.peekNext() // safeguard: fall back to default implementation

            val peekNext = cursor.peekNext()
            if (peekNext != null) {
                return peekNext
            }

            // we're at the end of the block. Check if there is a next block.
            val block = this.currentBlock
                ?: return null
            val nextBlockIndex = block.metaData.blockSequenceNumber + 1
            if (nextBlockIndex >= fileHeader.metaData.numberOfBlocks) {
                // there IS no next block. Peeking won't work.
                return null
            }

            // we're at the end of a block and there IS a next block. This means
            // we have to read the next block, read the first entry, and then come
            // back to this block. This is rather unfortunate from a performance
            // perspective.

            // the default implementation will take care of this.
            return super.peekNext()

        }

        override fun peekPrevious(): Pair<KeyAndTSN, Command>? {
            if (!this.isValidPosition) {
                return null
            }
            val cursor = this.currentCursor
                ?: return super.peekPrevious() // safeguard: fall back to default implementation

            val peekNext = cursor.peekPrevious()
            if(peekNext != null){
                return peekNext
            }

            // we're at the start of the block. Check if there is a previous block.
            val block = this.currentBlock
                ?: return null
            val previousBlockIndex = block.metaData.blockSequenceNumber - 1
            if (previousBlockIndex < 0) {
                // there IS no previous block. Peeking won't work.
                return null
            }

            // we're at the start of a block and there IS a previous block. This means
            // we have to read the previous block, read the last entry, and then come
            // back to this block. This is rather unfortunate from a performance
            // perspective.

            // the default implementation will take care of this.
            return super.peekPrevious()
        }

        override val keyOrNull: KeyAndTSN?
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

        override fun onClose(action: CloseHandler): Cursor<KeyAndTSN, Command> {
            check(this.isOpen, this::getAlreadyClosedMessage)
            this.closeHandlers += action
            return this
        }

        override fun close() {
            if (!this.isOpen) {
                return
            }
            this.isOpen = false
            val current = this.currentCursor
            val currentCursorCloseHandler = if (current != null) {
                current::close
            } else {
                null
            }
            CursorUtils.executeCloseHandlers(currentCursorCloseHandler, this.closeHandlers)
        }

        override fun seekExactlyOrNext(key: KeyAndTSN): Boolean {
            check(this.isOpen, this::getAlreadyClosedMessage)
            ChronoStoreStatistics.FILE_CURSOR_EXACTLY_OR_NEXT_SEEKS.incrementAndGet()
            if (key == this.keyOrNull) {
                // we're already there
                return true
            }
            this.invalidatePosition()
            val blockIndex = fileHeader.indexOfBlocks.getBlockIndexForKeyAndTimestampAscending(key)
                ?: return false
            val block = getBlockForIndex(blockIndex)
                ?: return false
            val cursor = block.cursor()

            if (!cursor.seekExactlyOrNext(key)) {
                cursor.close()
                return false
            }
            this.currentBlock = block
            this.currentCursor = cursor
            this.isValidPosition = true
            return true
        }

        override fun seekExactlyOrPrevious(key: KeyAndTSN): Boolean {
            check(this.isOpen, this::getAlreadyClosedMessage)
            ChronoStoreStatistics.FILE_CURSOR_EXACTLY_OR_PREVIOUS_SEEKS.incrementAndGet()
            if (key == this.keyOrNull) {
                // we're already there
                return true
            }
            this.invalidatePosition()

            val blockIndex = fileHeader.indexOfBlocks.getBlockIndexForKeyAndTimestampDescending(key)
                ?: return false
            val block = getBlockForIndex(blockIndex)
                ?: return false
            val cursor = block.cursor()
            if (!cursor.seekExactlyOrPrevious(key)) {
                cursor.close()
                return false
            }
            this.currentBlock = block
            this.currentCursor = cursor
            this.isValidPosition = true
            return true
        }

        private fun getAlreadyClosedMessage(): String {
            return "This cursor on ${this.driver.filePath} has already been closed!"
        }

        override fun toString(): String {
            return "ChronoStoreFileCursor[${this@ChronoStoreFileReader.driver.filePath}]"
        }
    }
}