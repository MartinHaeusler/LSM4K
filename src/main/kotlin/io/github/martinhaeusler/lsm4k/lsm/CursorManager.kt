package io.github.martinhaeusler.lsm4k.lsm

import io.github.martinhaeusler.lsm4k.api.LSM4KTransaction
import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.model.command.KeyAndTSN
import io.github.martinhaeusler.lsm4k.util.cursor.Cursor
import io.github.martinhaeusler.lsm4k.util.cursor.CursorInternal
import io.github.martinhaeusler.lsm4k.util.cursor.LevelOrTierCursor
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class CursorManager {

    private val fileNameToOpenCursors = mutableMapOf<String, MutableSet<CursorAndTransaction>>()
    private val lock = ReentrantReadWriteLock(true)

    fun openCursorOnLevelOrTier(transaction: LSM4KTransaction, sortedFiles: List<LSMTreeFile>): CursorInternal<KeyAndTSN, Command> {
        this.lock.write {
            val levelOrTierCursor = LevelOrTierCursor(
                sortedFiles = sortedFiles,
                createNewCursor = { file ->
                    this.openCursorOnFile(transaction, file)
                }
            )

            // register this level/tier cursor for EVERY file in that level or tier. This will protect
            // the files from being garbage collected while that cursor is active.
            for (lsmTreeFile in sortedFiles) {
                val treeFileName = lsmTreeFile.virtualFile.name
                fileNameToOpenCursors.getOrPut(treeFileName, ::mutableSetOf) += CursorAndTransaction(levelOrTierCursor, transaction)
            }

            levelOrTierCursor.onClose { this.closeCursorOnLevelOrTier(transaction, sortedFiles, levelOrTierCursor) }
            return levelOrTierCursor
        }
    }

    private fun closeCursorOnLevelOrTier(transaction: LSM4KTransaction, files: List<LSMTreeFile>, cursor: Cursor<KeyAndTSN, Command>) {
        val cursorAndTransaction = CursorAndTransaction(cursor, transaction)
        this.lock.write {
            for (file in files) {
                val fileName = file.virtualFile.name
                val cursorSet = fileNameToOpenCursors[fileName]
                    ?: continue
                cursorSet.remove(cursorAndTransaction)
                if (cursorSet.isEmpty()) {
                    fileNameToOpenCursors.remove(fileName)
                }
            }
        }
    }

    fun openCursorOnFile(transaction: LSM4KTransaction, lsmTreeFile: LSMTreeFile): CursorInternal<KeyAndTSN, Command> {
        this.lock.write {
            val rawCursor = lsmTreeFile.cursor()
            val cursorAndTransaction = CursorAndTransaction(rawCursor, transaction)
            val treeFileName = lsmTreeFile.virtualFile.name
            fileNameToOpenCursors.getOrPut(treeFileName, ::mutableSetOf) += cursorAndTransaction
            return rawCursor.onClose { closeCursor(treeFileName, transaction, rawCursor) } as CursorInternal<KeyAndTSN, Command>
        }
    }

    fun closeAllCursors(transaction: LSM4KTransaction) {
        this.lock.write {
            val affectedCursors = this.fileNameToOpenCursors.values.asSequence()
                .flatten()
                .filter { it.transaction == transaction }
                .map { it.cursor }
                .toList()
            for (cursor in affectedCursors) {
                // this will trigger the "onClose()", which will trigger "closeCursor" and do the cleanup.
                cursor.close()
            }
        }
    }

    fun hasOpenCursorOnFile(fileName: String): Boolean {
        this.lock.read {
            return !this.fileNameToOpenCursors[fileName].isNullOrEmpty()
        }
    }

    private fun closeCursor(fileName: String, transaction: LSM4KTransaction, cursor: Cursor<KeyAndTSN, Command>) {
        val cursorAndTransaction = CursorAndTransaction(cursor, transaction)
        this.lock.write {
            val cursorSet = fileNameToOpenCursors[fileName]
            if (cursorSet != null) {
                cursorSet.remove(cursorAndTransaction)
                if (cursorSet.isEmpty()) {
                    fileNameToOpenCursors.remove(fileName)
                }
            }
        }
    }

    private data class CursorAndTransaction(
        val cursor: Cursor<KeyAndTSN, Command>,
        val transaction: LSM4KTransaction,
    )

}