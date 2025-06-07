package io.github.martinhaeusler.lsm4k.util.cursor

import io.github.martinhaeusler.lsm4k.lsm.LSMTreeFile
import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.model.command.KeyAndTSN
import io.github.martinhaeusler.lsm4k.util.cursor.CursorUtils.checkIsOpen

class LevelOrTierCursor(
    private val sortedFiles: List<LSMTreeFile>,
    private val createNewCursor: (LSMTreeFile) -> Cursor<KeyAndTSN, Command> = LSMTreeFile::cursor,
) : CursorInternal<KeyAndTSN, Command> {

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
    private val closeHandlers = mutableListOf<CloseHandler>()

    private var currentFileListIndex: Int? = null
    private var currentFileCursor: CursorInternal<KeyAndTSN, Command>? = null

    override val keyOrNull: KeyAndTSN?
        get() {
            this.checkIsOpen()
            return this.currentFileCursor?.keyOrNull
        }

    override val valueOrNull: Command?
        get() {
            this.checkIsOpen()
            return this.currentFileCursor?.valueOrNull
        }

    override val isValidPosition: Boolean
        get() = this.currentFileCursor != null

    init {
        require(sortedFiles.isNotEmpty()) {
            "Cannot create a SortedRunCursor on an empty file list!"
        }
    }

    override fun invalidatePositionInternal() {
        this.checkIsOpen()
        this.dropCurrentCursor()
    }

    override fun firstInternal(): Boolean {
        this.checkIsOpen()
        val cursor = this.ensureCursorOn(0)
        return cursor.firstInternal()
    }

    override fun lastInternal(): Boolean {
        this.checkIsOpen()
        return ensureCursorOn(sortedFiles.lastIndex).lastInternal()
    }

    override fun nextInternal(): Boolean {
        this.checkIsOpen()
        val cursor = this.currentFileCursor
            ?: throw IllegalStateException("No cursor currently open!")

        // move that cursor in the given direction
        val moved = cursor.nextInternal()
        if (moved) {
            // move worked, everything is ok
            return true
        }

        // the move did not work. That means the current cursor has
        // hit the start or end of its file. We may need to move on
        // to the next file instead.

        val currentFileIndex = this.currentFileListIndex
            ?: throw IllegalStateException("No current file list index!")

        // are we at the last file?
        return if (currentFileIndex >= this.sortedFiles.lastIndex) {
            // we're indeed at the last file. Nothing we can do, can't move there.
            // also, we keep the current cursor.
            false
        } else {
            // we're NOT at the last file. Grab the cursor for the next file, skipping over empty files.
            this.moveToNextHigherFile(currentFileIndex)
        }
    }


    override fun previousInternal(): Boolean {
        this.checkIsOpen()
        val cursor = this.currentFileCursor
            ?: throw IllegalStateException("No cursor currently open!")

        // move that cursor in the given direction
        val moved = cursor.previousInternal()
        if (moved) {
            // move worked, everything is ok
            return true
        }

        // the move did not work. That means the current cursor has
        // hit the start or end of its file. We may need to move on
        // to the previous file instead.

        val currentFileIndex = this.currentFileListIndex
            ?: throw IllegalStateException("No current file list index!")

        // are we at the first file?
        return if (currentFileIndex <= 0) {
            // we're indeed at the first file. Nothing we can do, can't move there.
            // also, we keep the current cursor.
            false
        } else {
            // we're NOT at the first file. Grab the cursor for the previous file, skipping over empty files.
            this.moveToNextLowerFile(currentFileIndex)
        }
    }

    private fun moveToNextHigherFile(startFileIndex: Int): Boolean {
        var currentFileIndex = startFileIndex
        while (true) {
            currentFileIndex++
            if (currentFileIndex > this.sortedFiles.lastIndex) {
                // there is no newer non-empty file, go back to our start file
                return this.ensureCursorOn(startFileIndex).lastInternal()
            }
            val nextFileCursor = this.ensureCursorOn(currentFileIndex)
            if (!nextFileCursor.firstInternal()) {
                // the next file is empty, skip it
                continue
            } else {
                // we've found a non-empty file, all good!
                return true
            }
        }
    }

    private fun moveToNextLowerFile(startFileIndex: Int): Boolean {
        var currentFileIndex = startFileIndex
        while (true) {
            currentFileIndex--
            if (currentFileIndex < 0) {
                // there is no newer non-empty file, go back to our start file
                return this.ensureCursorOn(startFileIndex).lastInternal()
            }
            val previousFileCursor = this.ensureCursorOn(currentFileIndex)
            if (!previousFileCursor.lastInternal()) {
                // the previous file is empty, skip it
                continue
            } else {
                // we've found a non-empty file, all good!
                return true
            }
        }
    }

    override fun seekExactlyOrPreviousInternal(key: KeyAndTSN): Boolean {
        this.checkIsOpen()
        for (fileListIndex in this.sortedFiles.indices.reversed()) {
            val file = this.sortedFiles[fileListIndex]
            val metadata = file.header.metaData
            val minKey = metadata.minKey
                ?: continue // empty file, ignore
            val maxKey = metadata.maxKey
                ?: continue // empty file, ignore

            if (minKey > key.key) {
                // all keys in this file are too big, ignore
                continue
            }

            if (maxKey < key.key) {
                // we've been looking for a key in between files which doesn't exist,
                // so we have to use the next-smaller key, which is the end key of this file.
                val cursor = this.ensureCursorOn(fileListIndex)
                check(cursor.lastInternal()) {
                    "Illegal Iterator state - move 'last()' failed!"
                }

                return true
            }

            // the key might be in here
            val cursor = this.ensureCursorOn(fileListIndex)
            val found = cursor.seekExactlyOrPreviousInternal(key)
            if (found) {
                return true
            } else {
                // try the next file
                continue
            }
        }
        // we've exhausted all our files and found nothing.
        this.dropCurrentCursor()
        return false
    }

    override fun seekExactlyOrNextInternal(key: KeyAndTSN): Boolean {
        this.checkIsOpen()
        for (fileListIndex in this.sortedFiles.indices) {
            val file = this.sortedFiles[fileListIndex]
            val metadata = file.header.metaData
            val minKey = metadata.minKey
                ?: continue // empty file, ignore
            val maxKey = metadata.maxKey
                ?: continue // empty file, ignore

            if (maxKey < key.key) {
                // all keys in this file are too small, ignore
                continue
            }

            if (minKey > key.key) {
                // we've been looking for a key in between files which doesn't exist,
                // so we have to use the next-higher key, which is the start key of this file.
                val cursor = this.ensureCursorOn(fileListIndex)
                check(cursor.firstInternal()) {
                    "Illegal Iterator state - move 'first()' failed!"
                }
                return true
            }

            // the key might be in here
            val cursor = this.ensureCursorOn(fileListIndex)
            val found = cursor.seekExactlyOrNextInternal(key)
            if (found) {
                return true
            } else {
                // try the next file
                continue
            }
        }
        // we've exhausted all our files and found nothing.
        this.dropCurrentCursor()
        return false
    }

    private fun ensureCursorOn(fileListIndex: Int): CursorInternal<KeyAndTSN, Command> {
        require(fileListIndex in this.sortedFiles.indices) {
            "Argument 'fileListIndex' (${fileListIndex}) is out of range [0..${this.sortedFiles.lastIndex}]!"
        }
        val currentFileIndex = this.currentFileListIndex
        val currentCursor = this.currentFileCursor
        if (currentFileIndex == fileListIndex && currentCursor != null) {
            // keep using the current cursor, no need to create a new one.
            return currentCursor
        }

        // we need another cursor, drop the current one
        this.dropCurrentCursor()

        val file = this.sortedFiles[fileListIndex]

        val newCursor = this.createNewCursor(file) as CursorInternal<KeyAndTSN, Command>
        newCursor.parent = this
        this.currentFileCursor = newCursor
        this.currentFileListIndex = fileListIndex
        return newCursor
    }

    private fun dropCurrentCursor() {
        this.currentFileCursor?.closeInternal()
        this.currentFileCursor = null
    }

    override fun onClose(action: CloseHandler): LevelOrTierCursor {
        this.checkIsOpen()
        this.closeHandlers += action
        return this
    }

    override fun closeInternal() {
        if (!this.isOpen) {
            return
        }
        this.isOpen = false
        val currentInnerCursor = this.currentFileCursor
        val innerClose = if (currentInnerCursor == null) {
            null
        } else {
            currentInnerCursor::closeInternal
        }
        CursorUtils.executeCloseHandlers(innerClose, this.closeHandlers)
    }
}