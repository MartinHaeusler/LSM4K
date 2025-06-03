package org.chronos.chronostore.io.format.cursor

import org.chronos.chronostore.io.format.BlockLoader
import org.chronos.chronostore.io.format.FileHeader
import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTSN
import org.chronos.chronostore.util.cursor.CloseHandler
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.CursorInternal
import org.chronos.chronostore.util.cursor.CursorUtils
import org.chronos.chronostore.util.cursor.CursorUtils.checkIsOpen

class ChronoStoreFileCursor(
    private val file: VirtualFile,
    private val fileHeader: FileHeader,
    private val blockLoader: BlockLoader,
) : CursorInternal<KeyAndTSN, Command> {

    companion object {

        const val CURSOR_NAME = "FileCursor"

        private const val COMMAND_BUFFER_SIZE = 10

    }

    override var parent: CursorInternal<*, *>? = null
        set(value) {
            if (field === value) {
                return
            }
            check(field == null) {
                "Cannot assign another parent to this cursor; a parent is already present." +
                    " Existing parent: ${field}, proposed new parent: ${value}"
            }
            field = value
        }

    override var isOpen: Boolean = true

    override var isValidPosition: Boolean = false

    private var currentBlock: DataBlock? = null
    private var currentCursor: CursorInternal<KeyAndTSN, Command>? = null

    private val closeHandlers = mutableListOf<CloseHandler>()

    private val prefetcher: Prefetcher = Prefetcher(
        file = this.file,
        fileHeader = this.fileHeader,
        blockLoader = this.blockLoader,
        commandBufferSize = COMMAND_BUFFER_SIZE,
    )

    override val keyOrNull: KeyAndTSN?
        get() {
            this.checkIsOpen()
            if (!this.isValidPosition) {
                return null
            }
            return this.currentCursor?.keyOrNull
        }

    override val valueOrNull: Command?
        get() {
            this.checkIsOpen()
            if (!this.isValidPosition) {
                return null
            }
            return this.currentCursor?.valueOrNull
        }

    override fun invalidatePositionInternal() {
        this.checkIsOpen()
        this.currentCursor?.close()
        this.currentCursor = null
        this.currentBlock = null
        this.isValidPosition = false
    }

    override fun firstInternal(): Boolean {
        this.checkIsOpen()
        this.prefetcher.registerOperation(CursorMoveOperation.FIRST, null)
        this.invalidatePositionInternal()
        val firstBlock = this.blockLoader.getBlockOrNull(file, 0)
        if (firstBlock == null || firstBlock.isEmpty()) {
            this.isValidPosition = false
            return false
        }
        this.currentBlock = firstBlock
        val cursor = firstBlock.cursor() as CursorInternal<KeyAndTSN, Command>
        // the block isn't empty, so we have a first element.
        check(cursor.firstInternal()) {
            "Illegal Iterator state - move 'first()' failed!"
        }
        this.currentCursor = cursor
        cursor.parent = this
        this.isValidPosition = true
        return true
    }

    override fun lastInternal(): Boolean {
        this.checkIsOpen()
        this.prefetcher.registerOperation(CursorMoveOperation.LAST, null)
        this.invalidatePositionInternal()
        val numberOfBlocks = fileHeader.metaData.numberOfBlocks
        val lastBlock = this.blockLoader.getBlockOrNull(file, numberOfBlocks - 1)
        if (lastBlock == null || lastBlock.isEmpty()) {
            this.isValidPosition = false
            return false
        }
        this.currentBlock = lastBlock
        val cursor = lastBlock.cursor() as CursorInternal<KeyAndTSN, Command>
        this.currentCursor = cursor
        cursor.parent = this
        // the block isn't empty, so we have a last element.
        check(cursor.lastInternal()) {
            "Illegal Iterator state - move 'last()' failed!"
        }
        this.isValidPosition = true
        return true
    }

    override fun nextInternal(): Boolean {
        this.checkIsOpen()
        this.prefetcher.registerOperation(CursorMoveOperation.NEXT, this.currentBlock)
        if (!this.isValidPosition) {
            return false
        }
        val block = this.currentBlock
        val cursor = this.currentCursor
        if (block == null || cursor == null) {
            return false
        }
        if (cursor.nextInternal()) {
            return true
        }
        // open the cursor on the next block
        val nextBlock = this.prefetcher.getNextBlock(block)
        if (nextBlock == null) {
            // there is no next position; keep the current cursor
            return false
        }

        val newCursor = nextBlock.cursor() as CursorInternal<KeyAndTSN, Command>
        newCursor.parent = this
        this.currentCursor = newCursor
        this.currentBlock = nextBlock
        check(newCursor.firstInternal()) {
            "Illegal Iterator state - move 'first()' failed!"
        }
        return true
    }

    override fun previousInternal(): Boolean {
        this.checkIsOpen()
        this.prefetcher.registerOperation(CursorMoveOperation.PREVIOUS, this.currentBlock)
        if (!this.isValidPosition) {
            return false
        }
        val block = this.currentBlock
        val cursor = this.currentCursor
        if (block == null || cursor == null) {
            return false
        }
        if (cursor.previousInternal()) {
            return true
        }
        // open the cursor on the previous block
        val previousBlock = this.prefetcher.getPreviousBlock(block)
        if (previousBlock == null) {
            // there is no previous position; keep the current cursor
            return false
        }

        val newCursor = previousBlock.cursor() as CursorInternal<KeyAndTSN, Command>
        check(newCursor.lastInternal()) {
            "Illegal Iterator state - move 'last()' failed!"
        }
        this.currentBlock = previousBlock
        this.currentCursor = newCursor
        newCursor.parent = this
        return true
    }

    override fun peekNextInternal(): Pair<KeyAndTSN, Command>? {
        this.checkIsOpen()
        if (!this.isValidPosition) {
            return null
        }
        val cursor = this.currentCursor
            ?: return super.peekNextInternal() // safeguard: fall back to default implementation

        val peekNext = cursor.peekNextInternal()
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
        return super.peekNextInternal()

    }

    override fun peekPreviousInternal(): Pair<KeyAndTSN, Command>? {
        this.checkIsOpen()
        if (!this.isValidPosition) {
            return null
        }
        val cursor = this.currentCursor
            ?: return super.peekPreviousInternal() // safeguard: fall back to default implementation

        val peekNext = cursor.peekPreviousInternal()
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
        return super.peekPreviousInternal()
    }

    override fun seekExactlyOrNextInternal(key: KeyAndTSN): Boolean {
        this.checkIsOpen()
        this.prefetcher.registerOperation(CursorMoveOperation.SEEK_NEXT, this.currentBlock)
        if (key == this.keyOrNull) {
            // we're already there
            return true
        }
        this.invalidatePositionInternal()
        val blockIndex = fileHeader.indexOfBlocks.getBlockIndexForKeyAndTimestampAscending(key)
            ?: return false
        val block = this.blockLoader.getBlockOrNull(this.file, blockIndex)
            ?: return false
        val cursor = block.cursor() as CursorInternal<KeyAndTSN, Command>

        if (!cursor.seekExactlyOrNextInternal(key)) {
            cursor.closeInternal()
            return false
        }
        this.currentBlock = block
        this.currentCursor = cursor
        cursor.parent = this
        this.isValidPosition = true
        return true
    }

    override fun seekExactlyOrPreviousInternal(key: KeyAndTSN): Boolean {
        this.checkIsOpen()
        this.prefetcher.registerOperation(CursorMoveOperation.SEEK_PREVIOUS, this.currentBlock)
        if (key == this.keyOrNull) {
            // we're already there
            return true
        }
        this.invalidatePositionInternal()

        val blockIndex = fileHeader.indexOfBlocks.getBlockIndexForKeyAndTimestampDescending(key)
            ?: return false
        val block = this.blockLoader.getBlockOrNull(this.file, blockIndex)
            ?: return false
        val cursor = block.cursor() as CursorInternal<KeyAndTSN, Command>
        if (!cursor.seekExactlyOrPreviousInternal(key)) {
            cursor.closeInternal()
            return false
        }
        this.currentBlock = block
        this.currentCursor = cursor
        cursor.parent = this
        this.isValidPosition = true
        return true
    }

    override fun onClose(action: CloseHandler): Cursor<KeyAndTSN, Command> {
        this.checkIsOpen()
        this.closeHandlers += action
        return this
    }

    override fun closeInternal() {
        if (!this.isOpen) {
            return
        }
        this.isOpen = false
        val current = this.currentCursor
        val currentCursorCloseHandler = if (current != null) {
            current::closeInternal
        } else {
            null
        }
        CursorUtils.executeCloseHandlers(currentCursorCloseHandler, this.closeHandlers)
    }


    override fun toString(): String {
        return "ChronoStoreFileCursor[${this.file.path}]"
    }

}
