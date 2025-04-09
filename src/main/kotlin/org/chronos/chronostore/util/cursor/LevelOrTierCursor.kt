package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.lsm.LSMTreeFile
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTSN
import org.chronos.chronostore.util.Order

class LevelOrTierCursor(
    private val sortedFiles: List<LSMTreeFile>,
    private val createNewCursor: (LSMTreeFile) -> Cursor<KeyAndTSN, Command> = LSMTreeFile::cursor,
) : AbstractCursor<KeyAndTSN, Command>() {

    private var currentFileListIndex: Int? = null
    private var currentFileCursor: Cursor<KeyAndTSN, Command>? = null


    init {
        require(sortedFiles.isNotEmpty()) {
            "Cannot create a SortedRunCursor on an empty file list!"
        }
    }

    override val keyOrNullInternal: KeyAndTSN?
        get() = this.currentFileCursor?.keyOrNull

    override val valueOrNullInternal: Command?
        get() = this.currentFileCursor?.valueOrNull

    override fun closeInternal() {
        this.currentFileCursor?.close()
    }

    override fun firstInternal(): Boolean {
        val cursor = this.ensureCursorOn(0)
        return cursor.first()
    }

    override fun lastInternal(): Boolean {
        val cursor = this.ensureCursorOn(this.sortedFiles.lastIndex)
        return cursor.last()
    }

    override fun moveInternal(direction: Order): Boolean {
        val cursor = this.currentFileCursor
            ?: throw IllegalStateException("No cursor currently open!")

        // move that cursor in the given direction
        val moved = cursor.move(direction)
        if (moved) {
            // move worked, everything is ok
            return true
        }

        // the move did not work. That means the current cursor has
        // hit the start or end of its file. We may need to move on
        // to the next / previous file instead.

        val currentFileIndex = this.currentFileListIndex
            ?: throw IllegalStateException("No current file list index!")

        return when (direction) {
            Order.ASCENDING -> {
                // are we at the last file?
                if (currentFileIndex >= this.sortedFiles.lastIndex) {
                    // we're indeed at the last file. Nothing we can do, can't move there.
                    // also, we keep the current cursor.
                    false
                } else {
                    // we're NOT at the last file. Grab the cursor for the next file, skipping over empty files.
                    this.moveToNextHigherFile(currentFileIndex)
                }
            }

            Order.DESCENDING -> {
                // are we at the first file?
                if (currentFileIndex <= 0) {
                    // we're indeed at the first file. Nothing we can do, can't move there.
                    // also, we keep the current cursor.
                    false
                } else {
                    // we're NOT at the first file. Grab the cursor for the previous file, skipping over empty files.
                    this.moveToNextLowerFile(currentFileIndex)
                }
            }
        }

    }

    private fun moveToNextHigherFile(startFileIndex: Int): Boolean {
        var currentFileIndex = startFileIndex
        while (true) {
            currentFileIndex++
            if (currentFileIndex > this.sortedFiles.lastIndex) {
                // there is no newer non-empty file, go back to our start file
                return this.ensureCursorOn(startFileIndex).last()
            }
            val nextFileCursor = this.ensureCursorOn(currentFileIndex)
            if (!nextFileCursor.first()) {
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
                return this.ensureCursorOn(startFileIndex).last()
            }
            val previousFileCursor = this.ensureCursorOn(currentFileIndex)
            if (!previousFileCursor.last()) {
                // the previous file is empty, skip it
                continue
            } else {
                // we've found a non-empty file, all good!
                return true
            }
        }
    }

    override fun seekExactlyOrPreviousInternal(key: KeyAndTSN): Boolean {
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
                cursor.lastOrThrow()
                return true
            }

            // the key might be in here
            val cursor = this.ensureCursorOn(fileListIndex)
            val found = cursor.seekExactlyOrPrevious(key)
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
                cursor.firstOrThrow()
                return true
            }

            // the key might be in here
            val cursor = this.ensureCursorOn(fileListIndex)
            val found = cursor.seekExactlyOrNext(key)
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

    private fun ensureCursorOn(fileListIndex: Int): Cursor<KeyAndTSN, Command> {
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

        val newCursor = this.createNewCursor(file)
        this.currentFileCursor = newCursor
        this.currentFileListIndex = fileListIndex
        return newCursor
    }

    private fun dropCurrentCursor() {
        this.currentFileCursor?.close()
        this.currentFileCursor = null
    }

}