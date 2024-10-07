package org.chronos.chronostore.lsm

import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTSN
import org.chronos.chronostore.util.cursor.Cursor
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class CursorManager {

    private val fileNameToOpenCursors = mutableMapOf<String, MutableSet<CursorAndTransaction>>()
    private val lock = ReentrantReadWriteLock(true)

    fun openCursorOn(transaction: ChronoStoreTransaction, lsmTreeFile: LSMTreeFile): Cursor<KeyAndTSN, Command> {
        this.lock.write {
            val rawCursor = lsmTreeFile.cursor()
            val cursorAndTransaction = CursorAndTransaction(rawCursor, transaction)
            val treeFileName = lsmTreeFile.virtualFile.name
            fileNameToOpenCursors.getOrPut(treeFileName, ::mutableSetOf) += cursorAndTransaction
            return rawCursor.onClose { closeCursor(treeFileName, transaction, rawCursor) }
        }
    }

    fun closeAllCursors(transaction: ChronoStoreTransaction) {
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

    private fun closeCursor(fileName: String, transaction: ChronoStoreTransaction, cursor: Cursor<KeyAndTSN, Command>) {
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
        val transaction: ChronoStoreTransaction
    )

}