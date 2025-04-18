package org.chronos.chronostore.io.format.cursor

import org.chronos.chronostore.io.format.BlockLoader
import org.chronos.chronostore.io.format.FileHeader
import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTSN
import org.chronos.chronostore.util.cursor.CloseHandler
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.CursorUtils
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics

class ChronoStoreFileCursor(
    private val file: VirtualFile,
    private val fileHeader: FileHeader,
    private val blockLoader: BlockLoader,
) : Cursor<KeyAndTSN, Command> {

    companion object {

        private const val COMMAND_BUFFER_SIZE = 10


    }

    override var modCount: Long = 0

    override var isOpen: Boolean = true

    override var isValidPosition: Boolean = false

    private var currentBlock: DataBlock? = null
    private var currentCursor: Cursor<KeyAndTSN, Command>? = null

    private val closeHandlers = mutableListOf<CloseHandler>()

    private val prefetcher: Prefetcher = Prefetcher(
        file = this.file,
        fileHeader = this.fileHeader,
        blockLoader = this.blockLoader,
        commandBufferSize = COMMAND_BUFFER_SIZE,
    )

    init {
        ChronoStoreStatistics.FILE_CURSORS.incrementAndGet()
    }

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
        this.prefetcher.registerOperation(CursorMoveOperation.FIRST, null)
        ChronoStoreStatistics.FILE_CURSOR_FIRST_SEEKS.incrementAndGet()
        this.invalidatePosition()
        val firstBlock = this.blockLoader.getBlockOrNull(file, 0)
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
        this.prefetcher.registerOperation(CursorMoveOperation.LAST, null)
        ChronoStoreStatistics.FILE_CURSOR_LAST_SEEKS.incrementAndGet()
        this.invalidatePosition()
        val numberOfBlocks = fileHeader.metaData.numberOfBlocks
        val lastBlock = this.blockLoader.getBlockOrNull(file, numberOfBlocks - 1)
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
        this.prefetcher.registerOperation(CursorMoveOperation.NEXT, this.currentBlock)
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
        val nextBlock = this.prefetcher.getNextBlock(block)
        if (nextBlock == null) {
            // there is no next position; keep the current cursor
            this.modCount++
            return false
        }

        val newCursor = nextBlock.cursor()
        newCursor.firstOrThrow()
        this.currentBlock = nextBlock
        this.currentCursor = newCursor
        return true
    }

    override fun previous(): Boolean {
        check(this.isOpen, this::getAlreadyClosedMessage)
        this.prefetcher.registerOperation(CursorMoveOperation.PREVIOUS, this.currentBlock)
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
        val previousBlock = this.prefetcher.getPreviousBlock(block)
        if (previousBlock == null) {
            // there is no previous position; keep the current cursor
            this.modCount++
            return false
        }

        val newCursor = previousBlock.cursor()
        newCursor.lastOrThrow()
        this.currentBlock = previousBlock
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
        if (peekNext != null) {
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
        this.prefetcher.registerOperation(CursorMoveOperation.SEEK_NEXT, this.currentBlock)
        ChronoStoreStatistics.FILE_CURSOR_EXACTLY_OR_NEXT_SEEKS.incrementAndGet()
        if (key == this.keyOrNull) {
            // we're already there
            return true
        }
        this.invalidatePosition()
        val blockIndex = fileHeader.indexOfBlocks.getBlockIndexForKeyAndTimestampAscending(key)
            ?: return false
        val block = this.blockLoader.getBlockOrNull(this.file, blockIndex)
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
        this.prefetcher.registerOperation(CursorMoveOperation.SEEK_PREVIOUS, this.currentBlock)
        ChronoStoreStatistics.FILE_CURSOR_EXACTLY_OR_PREVIOUS_SEEKS.incrementAndGet()
        if (key == this.keyOrNull) {
            // we're already there
            return true
        }
        this.invalidatePosition()

        val blockIndex = fileHeader.indexOfBlocks.getBlockIndexForKeyAndTimestampDescending(key)
            ?: return false
        val block = this.blockLoader.getBlockOrNull(this.file, blockIndex)
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
        return "This cursor on ${this.file.path} has already been closed!"
    }

    override fun toString(): String {
        return "ChronoStoreFileCursor[${this.file.path}]"
    }

}
