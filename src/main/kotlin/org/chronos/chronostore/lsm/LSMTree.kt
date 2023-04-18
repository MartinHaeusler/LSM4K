package org.chronos.chronostore.lsm

import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.datablock.BlockReadMode
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.lsm.LSMTreeFile.Companion.FILE_EXTENSION
import org.chronos.chronostore.lsm.event.InMemoryLsmInsertEvent
import org.chronos.chronostore.lsm.event.LsmCursorClosedEvent
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.IndexBasedCursor
import org.chronos.chronostore.util.cursor.OverlayCursor
import java.io.File
import java.util.*
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class LSMTree(
    private val directory: VirtualDirectory,
    private val mergeStrategy: MergeStrategy,
    private val blockCache: LocalBlockCache,
    private val driverFactory: RandomFileAccessDriverFactory,
    private val blockReadMode: BlockReadMode,
) {

    private val inMemoryTree = ConcurrentSkipListMap<KeyAndTimestamp, Command>()
    private val lock = ReentrantReadWriteLock(true)

    private val fileList: MutableList<LSMTreeFile>

    private val openCursors = Collections.synchronizedSet(mutableSetOf<Cursor<KeyAndTimestamp, Command>>())

    init {
        if (!this.directory.exists()) {
            this.directory.mkdirs()
        }
        this.fileList = loadFileList()
    }

    private fun loadFileList(): MutableList<LSMTreeFile> {
        val fileList = this.directory.list().asSequence()
            .map { it.substringAfterLast(File.separatorChar) }
            .filter { it.endsWith(FILE_EXTENSION) }
            .mapNotNull {
                val index = parseFileIndexOrNull(it)
                    ?: return@mapNotNull null
                LSMTreeFile(
                    virtualFile = this.directory.file(it),
                    index = index,
                    driverFactory = this.driverFactory,
                    blockReadMode = this.blockReadMode,
                    blockCache = this.blockCache
                )
            }
            // sort by file index ascending
            .sortedBy { it.index }
            .toMutableList()

        if (fileList.isNotEmpty()) {
            // if we have a non-empty file list, there should be one file named "0.chronostore"
            val firstFile = fileList.first()
            check(firstFile.index == 0) {
                "LSM Tree Base file '0${FILE_EXTENSION}' is missing in directory '${this.directory.path}'!"
            }
        }
        return fileList
    }

    private fun parseFileIndexOrNull(fileName: String): Int? {
        return fileName
            .substringAfterLast(File.separatorChar)
            .removeSuffix(FILE_EXTENSION)
            .toIntOrNull()
    }

    fun get(keyAndTimestamp: KeyAndTimestamp): Command? {
        this.lock.read {
            // first, check the in-memory tree
            val inMemoryEntry = this.inMemoryTree.floorEntry(keyAndTimestamp)
            if (inMemoryEntry != null && inMemoryEntry.key.key == keyAndTimestamp.key) {
                return inMemoryEntry.value
            }
            // then, search the on-disk files (in reverse order, latest first)
            for (lsmTreeFile in this.fileList.asReversed()) {
                val result = lsmTreeFile.get(keyAndTimestamp)
                if (result != null) {
                    return result
                }
            }
            return null
        }
    }

    fun cursor(): Cursor<KeyAndTimestamp, Command> {
        val rawCursor = this.lock.read {
            val inMemoryDataList = this.inMemoryTree.toList()
            val inMemoryCursor = IndexBasedCursor(
                minIndex = 0,
                maxIndex = inMemoryDataList.lastIndex,
                getEntryAtIndex = inMemoryDataList::get,
                getCursorName = { "In-Memory Cursor" }
            )
            val cursors = this.fileList.asReversed().map { it.cursor() }.toMutableList()
            cursors.add(inMemoryCursor)
            val cursor = cursors.reduce { l, r -> OverlayCursor(l, r) }
            this.openCursors.add(cursor)
            cursor
        }
        return rawCursor.onClose {
            this.lock.read {
                this.openCursors.remove(rawCursor)
            }
            this.mergeStrategy.handleCursorClosedEvent(LsmCursorClosedEvent(this))
        }
    }

    fun put(commands: Iterable<Command>) {
        var inserted = 0
        val totalSize: Int
        this.lock.write {
            for (command in commands) {
                val keyAndTimestamp = command.keyAndTimestamp
                require(!this.inMemoryTree.containsKey(keyAndTimestamp)) {
                    "The key-and-timestamp ${keyAndTimestamp} is already contained in in-memory LSM tree!"
                }
                this.inMemoryTree[command.keyAndTimestamp] = command
                inserted += 1
            }
            totalSize = this.inMemoryTree.size
        }
        // let the merge strategy know what happened
        this.mergeStrategy.handleInMemoryInsertEvent(
            InMemoryLsmInsertEvent(
                this,
                inserted,
                totalSize,
            )
        )
    }

    fun getMaxPersistedTimestamp(): Timestamp {
        return fileList.asSequence()
            .mapNotNull { it.header.metaData.maxTimestamp }
            .maxOrNull() ?: -1
    }

}
