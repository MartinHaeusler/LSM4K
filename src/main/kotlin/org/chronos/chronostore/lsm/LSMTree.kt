package org.chronos.chronostore.lsm

import com.google.common.collect.Iterators
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTask
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.format.ChronoStoreFileWriter
import org.chronos.chronostore.io.format.datablock.BlockReadMode
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.chronos.chronostore.lsm.LSMTreeFile.Companion.FILE_EXTENSION
import org.chronos.chronostore.lsm.event.InMemoryLsmInsertEvent
import org.chronos.chronostore.lsm.event.LsmCursorClosedEvent
import org.chronos.chronostore.lsm.merge.strategy.MergeService
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.IndexBasedCursor
import org.chronos.chronostore.util.cursor.OverlayCursor
import org.chronos.chronostore.util.iterator.IteratorExtensions.checkOrdered
import org.chronos.chronostore.util.iterator.IteratorExtensions.orderedDistinct
import java.io.File
import java.util.*
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class LSMTree(
    private val directory: VirtualDirectory,
    private val mergeService: MergeService,
    private val blockCache: LocalBlockCache,
    private val driverFactory: RandomFileAccessDriverFactory,
    private val blockReadMode: BlockReadMode,
    private val newFileSettings: ChronoStoreFileSettings,
) {

    private val inMemoryTree = ConcurrentSkipListMap<KeyAndTimestamp, Command>()
    private val lock = ReentrantReadWriteLock(true)

    private val fileList: MutableList<LSMTreeFile>

    private val openCursors = Collections.synchronizedSet(mutableSetOf<Cursor<KeyAndTimestamp, Command>>())

    private val activeTaskLock = ReentrantReadWriteLock(true)
    private val activeTaskCondition = activeTaskLock.writeLock().newCondition()
    private var activeTaskMonitor: TaskMonitor? = null

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


    val inMemorySizeBytes: Long
        get() = this.inMemoryTree.values.sumOf { it.byteSize.toLong() }

    val path: String
        get() = this.directory.path


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
            this.mergeService.handleCursorClosedEvent(LsmCursorClosedEvent(this))
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
        this.mergeService.handleInMemoryInsertEvent(
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

    fun flushInMemoryDataToDisk(minFlushSizeBytes: Long, monitor: TaskMonitor) {
        this.performAsyncWriteTask(monitor) {
            monitor.reportStarted("Flushing Data to Disk")
            if (this.inMemorySizeBytes < minFlushSizeBytes) {
                // flush not necessary
                monitor.reportDone()
                return
            }
            val commands = monitor.subTask(0.1, "Collecting Entries to flush") {
                this.lock.write {
                    this.inMemoryTree.toMap()
                }
            }
            val newFileIndex = this.fileList.lastOrNull()?.index ?: 0
            val file = this.directory.file("${newFileIndex}${FILE_EXTENSION}")
            monitor.subTask(0.8, "Writing File") {
                file.deleteOverWriterFileIfExists()
                file.withOverWriter { overWriter ->
                    ChronoStoreFileWriter(
                        outputStream = overWriter.outputStream.buffered(),
                        settings = this.newFileSettings,
                        metadata = emptyMap()
                    ).use { writer ->
                        writer.writeFile(commands.values.iterator())
                    }
                    overWriter.commit()
                }
            }
            monitor.subTask(0.1, "Redirecting traffic to file") {
                this.lock.write {
                    this.fileList.add(LSMTreeFile(file, newFileIndex, this.driverFactory, this.blockReadMode, this.blockCache))
                    this.inMemoryTree.keys.removeAll(commands.keys)
                }
            }
            monitor.reportDone()
        }
    }


    fun mergeFiles(fileIndices: Set<Int>, monitor: TaskMonitor) {
        this.performAsyncWriteTask(monitor) {
            monitor.reportStarted("Merging ${fileIndices.size} files")
            val filesToMerge = mutableListOf<LSMTreeFile>()
            var previousFile: LSMTreeFile? = null
            for (file in this.fileList) {
                if (filesToMerge.isNotEmpty() && previousFile != null && previousFile.index !in fileIndices && file.index in fileIndices) {
                    // file list has holes
                    throw IllegalStateException(
                        "Set of files to merge is not continuous! Wanted to merge: ${
                            fileIndices.sorted().joinToString()
                        }, missing intermediate index: ${file.index}. Aborting merge."
                    )
                }
                if (file.index in fileIndices) {
                    filesToMerge += file
                }
                previousFile = file
            }

            val files = fileIndices.sorted().mapNotNull { index -> this.fileList.firstOrNull { file -> file.index == index } }
            if (files.size < 2) {
                monitor.reportDone()
                return
            }

            // this only works because the list of files has no "holes" in it (i.e. there is no intermediate file we don't merge)
            val targetFile = this.directory.file(filesToMerge.minBy { it.index }.virtualFile.name)

            targetFile.deleteOverWriterFileIfExists()
            targetFile.createOverWriter().use { overWriter ->
                val cursors = files.map { it.cursor() }
                try {
                    val iterators = cursors.mapNotNull {
                        if (!it.first()) {
                            it.ascendingValueSequenceFromHere().iterator()
                        } else {
                            null
                        }
                    }.toList()
                    val commands = Iterators.mergeSorted(iterators, Comparator.naturalOrder())
                    // ensure ordering and remove duplicates (which is cheap and lazy for ordered iterators)
                    val iterator = commands.checkOrdered(strict = false).orderedDistinct()
                    ChronoStoreFileWriter(
                        outputStream = overWriter.outputStream.buffered(),
                        settings = this.newFileSettings,
                        metadata = emptyMap()
                    ).use { writer ->
                        writer.writeFile(iterator)
                    }
                } finally {
                    for (cursor in cursors) {
                        cursor.close()
                    }
                }

                this.lock.write {
                    overWriter.commit()

                    this.fileList.removeAll(files.filter { it.virtualFile.path != targetFile.path })

                }
            }



        }
    }

    private inline fun <T> performAsyncWriteTask(monitor: TaskMonitor, task: () -> T): T {
        this.activeTaskLock.write {
            while (this.activeTaskMonitor != null) {
                this.activeTaskCondition.awaitUninterruptibly()
            }
            this.activeTaskMonitor = monitor
        }
        try {
            return task()
        } finally {
            this.activeTaskLock.write {
                this.activeTaskMonitor = null
                this.activeTaskCondition.signalAll()
            }
        }
    }

}
