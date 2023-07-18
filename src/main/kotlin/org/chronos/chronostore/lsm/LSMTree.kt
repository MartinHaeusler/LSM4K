package org.chronos.chronostore.lsm

import com.google.common.collect.Iterators
import mu.KotlinLogging
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.forEach
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTask
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.format.ChronoStoreFileWriter
import org.chronos.chronostore.io.format.datablock.BlockReadMode
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.chronos.chronostore.lsm.LSMTreeFile.Companion.FILE_EXTENSION
import org.chronos.chronostore.lsm.event.InMemoryLsmFlushEvent
import org.chronos.chronostore.lsm.event.InMemoryLsmInsertEvent
import org.chronos.chronostore.lsm.event.LsmCursorClosedEvent
import org.chronos.chronostore.lsm.merge.strategy.MergeService
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.cursor.*
import org.chronos.chronostore.util.iterator.IteratorExtensions.checkOrdered
import org.chronos.chronostore.util.iterator.IteratorExtensions.filter
import org.chronos.chronostore.util.iterator.IteratorExtensions.latestVersionOnly
import org.chronos.chronostore.util.iterator.IteratorExtensions.orderedDistinct
import org.chronos.chronostore.util.log.LogMarkers
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import org.chronos.chronostore.util.unit.BinarySize
import org.chronos.chronostore.util.unit.Bytes
import org.pcollections.TreePMap
import org.slf4j.MarkerFactory
import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.system.measureTimeMillis

class LSMTree(
    private val directory: VirtualDirectory,
    private val mergeService: MergeService,
    private val blockCache: LocalBlockCache,
    private val driverFactory: RandomFileAccessDriverFactory,
    private val blockReadMode: BlockReadMode,
    private val newFileSettings: ChronoStoreFileSettings,
    private val maxInMemoryTreeSize: BinarySize,
) {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    @Volatile
    private var inMemoryTree = TreePMap.empty<KeyAndTimestamp, Command>()
    private val inMemoryTreeSizeInBytes = AtomicLong(0)
    private val lock = ReentrantReadWriteLock(true)
    private val treeSizeChangedCondition = this.lock.writeLock().newCondition()

    private val fileList: MutableList<LSMTreeFile>
    private val nextFreeFileIndex = AtomicInteger(0)

    private val cursorManager = CursorManager()

    private val activeTaskLock = ReentrantReadWriteLock(true)
    private val activeTaskCondition = activeTaskLock.writeLock().newCondition()

    @Volatile
    private var activeTaskMonitor: TaskMonitor? = null

    private var garbageFileManager: GarbageFileManager

    init {
        if (!this.directory.exists()) {
            this.directory.mkdirs()
        }
        this.garbageFileManager = GarbageFileManager(this.directory.file(GarbageFileManager.FILE_NAME))
        this.fileList = loadFileList()
        this.nextFreeFileIndex.set(this.fileList.maxOfOrNull { it.index } ?: 0)
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


    val inMemorySize: BinarySize
        get() = this.inMemoryTreeSizeInBytes.get().Bytes

    val path: String
        get() = this.directory.path

    val allFiles: List<LSMTreeFile>
        get() = this.lock.read { this.fileList.toList() }

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

    fun openCursor(transaction: ChronoStoreTransaction, timestamp: Timestamp): Cursor<Bytes, Command> {
        require(timestamp <= transaction.lastVisibleTimestamp) { "Cannot open cursor on timestamp ${timestamp}, because the last visible timestamp in the transaction is ${transaction.lastVisibleTimestamp}!" }
        val rawCursor = this.lock.read {
            val inMemoryDataMap = this.inMemoryTree
            val inMemoryCursor = if (inMemoryDataMap.isEmpty()) {
                EmptyCursor { "In-Memory Cursor" }
            } else {
                NavigableMapCursor(inMemoryDataMap)
            }
            val fileCursors = this.fileList.asSequence().map { this.cursorManager.openCursorOn(transaction, it) }.toMutableList()
            if (fileCursors.isEmpty()) {
                return VersioningCursor(inMemoryCursor, timestamp, includeDeletions = true)
            }
            if (inMemoryCursor !is EmptyCursor) {
                fileCursors.add(inMemoryCursor)
            }
            val cursor = fileCursors.asSequence()
                .map { VersioningCursor(it, timestamp, includeDeletions = true) }
                .reduce(::OverlayCursor)
            cursor
        }
        return rawCursor.onClose {
            this.mergeService.handleCursorClosedEvent(LsmCursorClosedEvent(this))
        }
    }

    fun put(commands: Iterable<Command>) {
        val event = this.lock.write {
            val containedEntry = commands.asSequence().map { it.keyAndTimestamp }.firstOrNull(this.inMemoryTree::containsKey)
            require(containedEntry == null) {
                "The key-and-timestamp ${containedEntry} is already contained in in-memory LSM tree!"
            }

            // if the tree size gets too big, we have to block the writes until we have enough space
            // again. This is very unfortunate, but cannot be avoided. If the writer generates more
            // data per second than the flush task can write to disk, the only other alternative is
            // to claim more and more RAM until we eventually run into an OutOfMemoryError.
            this.waitUntilInMemoryTreeHasCapacity()

            // capture this value within the lock in order to avoid concurrency issues which
            // might cause "treeSizeBefore" to be greater than "treeSizeAfter"
            val treeSizeBefore = this.inMemorySize
            val treeCountBefore = this.inMemoryTree.size

            val keyAndTimestampToCommand = commands.associateBy { it.keyAndTimestamp }
            this.inMemoryTree = this.inMemoryTree.plusAll(keyAndTimestampToCommand)
            this.inMemoryTreeSizeInBytes.getAndAdd(keyAndTimestampToCommand.values.sumOf { it.byteSize }.toLong())

            val treeCountAfter = this.inMemoryTree.size
            val treeSizeAfter = this.inMemorySize

            // the in-memory tree size has changed, notify listeners
            this.treeSizeChangedCondition.signalAll()

            InMemoryLsmInsertEvent(
                lsmTree = this,
                inMemoryElementCountBefore = treeCountBefore,
                inMemoryElementCountAfter = treeCountAfter,
                inMemorySizeBefore = treeSizeBefore,
                inMemorySizeAfter = treeSizeAfter,
            )
        }
        // let the merge strategy know what happened
        this.mergeService.handleInMemoryInsertEvent(event)
    }

    private fun waitUntilInMemoryTreeHasCapacity() {
        var timeBeforeWriteStall = -1L
        while (this.inMemorySize >= this.maxInMemoryTreeSize) {
            // tree size is too big -> stall the write
            try {
                if (timeBeforeWriteStall < 0) {
                    timeBeforeWriteStall = System.nanoTime()
                    log.trace(LogMarkers.PERF) { "Stalling write to '${this.path}' because the in-memory tree cannot hold more data." }
                }
                this.treeSizeChangedCondition.await()
            } catch (e: Exception) {
                log.trace(LogMarkers.PERF) { "Caught exception while awaiting 'treeSizeChanged' condition: ${e}" }
            }
        }
        if (timeBeforeWriteStall >= 0) {
            // report the write stall time in the statistics
            val timeAfterWriteStall = System.nanoTime()
            val writeStallTime = (timeAfterWriteStall - timeBeforeWriteStall) / 1_000_000
            log.trace(LogMarkers.PERF) { "Write to '${this.path}' will no longer be stalled and may continue. Stall time: ${writeStallTime}ms." }
            ChronoStoreStatistics.TOTAL_WRITE_STALL_TIME_MILLIS.addAndGet(writeStallTime)
        }
    }

    fun getMaxPersistedTimestamp(): Timestamp {
        return fileList.asSequence()
            .mapNotNull { it.header.metaData.maxTimestamp }
            .maxOrNull() ?: -1
    }

    fun flushInMemoryDataToDisk(minFlushSize: BinarySize, monitor: TaskMonitor) {
        log.trace(LogMarkers.PERF) { "TASK: Flush in-memory data to disk" }
        this.performAsyncWriteTask(monitor) {
            monitor.reportStarted("Flushing Data to Disk")
            if (this.inMemorySize < minFlushSize) {
                // flush not necessary
                monitor.reportDone()
                return
            }
            log.trace(LogMarkers.PERF) { "Flushing LSM Tree '${this.directory}'!" }
            val commands = monitor.subTask(0.1, "Collecting Entries to flush") {
                this.inMemoryTree
            }
            val lastFileIndex = this.fileList.lastOrNull()?.index
            val newFileIndex = if (lastFileIndex == null) {
                0
            } else {
                lastFileIndex + 1
            }
            log.trace(LogMarkers.PERF) { "Target file index ${newFileIndex} will be used for flush of tree ${this.path}" }
            val file = this.directory.file("${newFileIndex}${FILE_EXTENSION}")
            val flushTime = measureTimeMillis {
                monitor.subTask(0.8, "Writing File") {
                    file.deleteOverWriterFileIfExists()
                    file.withOverWriter { overWriter ->
                        ChronoStoreFileWriter(
                            outputStream = overWriter.outputStream,
                            settings = this.newFileSettings,
                            metadata = emptyMap()
                        ).use { writer ->
                            log.trace(LogMarkers.PERF) { "Flushing ${commands.size} commands from in-memory segment into '${file.path}'." }
                            writer.writeFile(
                                // we're flushing this file from in-memory,
                                // so the resulting file has never been merged.
                                numberOfMerges = 0,
                                orderedCommands = commands.values.iterator()
                            )
                        }
                        overWriter.commit()
                    }
                }
            }
            log.trace(LogMarkers.PERF) { "Flush into index file ${newFileIndex} completed in ${flushTime}ms. Redirecting traffic to new data file for LSM tree ${this.path}" }
            val event = monitor.subTask(0.1, "Redirecting traffic to file") {
                this.lock.write {
                    this.fileList.add(LSMTreeFile(file, newFileIndex, this.driverFactory, this.blockReadMode, this.blockCache))
                    log.trace(LogMarkers.PERF) { "Removing ${commands.keys.size} keys from the in-memory tree (which has ${this.inMemoryTree.size} keys)..." }
                    this.inMemoryTree = this.inMemoryTree.minusAll(commands.keys)
                    log.trace(LogMarkers.PERF) { "${inMemoryTree.size} entries remaining in-memory after flush of tree ${this.path}" }
                    this.inMemoryTreeSizeInBytes.addAndGet(commands.values.sumOf { it.byteSize } * -1L)

                    // the in-memory tree size has changed, notify listeners
                    this.treeSizeChangedCondition.signalAll()

                    InMemoryLsmFlushEvent(
                        lsmTree = this,
                        flushedEntryCount = commands.size
                    )
                }
            }

            this.mergeService.handleInMemoryFlushEvent(event)
            monitor.reportDone()
        }
    }

    fun performGarbageCollection(storeName: String, taskMonitor: TaskMonitor) {
        taskMonitor.reportStarted("Collecting Garbage in '${storeName}'")
        val deleted = mutableListOf<String>()
        taskMonitor.forEach(1.0, "Deleting old files", this.garbageFileManager.garbageFiles) { fileName ->
            if (this.cursorManager.hasOpenCursorOnFile(fileName)) {
                // we must not delete any files we're currently iterating over.
                return@forEach
            }
            // nobody is using the file anymore, delete it.
            val file = this.directory.file(fileName)
            if (file.exists()) {
                file.deleteOverWriterFileIfExists()
                file.delete()
            }
            deleted += file.name
        }
        // remember that we deleted these files and don't need to try that again.
        this.garbageFileManager.removeAll(deleted)
        taskMonitor.reportDone()
    }

    fun mergeFiles(fileIndices: Set<Int>, retainOldVersions: Boolean, monitor: TaskMonitor) {
        log.debug { "TASK: merge files: ${fileIndices.sorted().joinToString(prefix = "[", separator = ", ", postfix = "]")}" }
        this.performAsyncWriteTask(monitor) {
            monitor.reportStarted("Merging ${fileIndices.size} files")
            val filesToMerge = this.getFilesToMerge(fileIndices)

            if (filesToMerge.size < 2) {
                monitor.reportDone()
                return
            }
            // get the file index we're going to use (nobody else will get the same index, because of the AtomicInteger used here)
            val targetFileIndex = this.nextFreeFileIndex.getAndIncrement()

            // this is the file we're going to write to
            val targetFile = this.directory.file(this.createFileNameForIndex(targetFileIndex))
            val maxMerge = filesToMerge.maxOfOrNull { it.header.metaData.numberOfMerges } ?: 0


            targetFile.deleteOverWriterFileIfExists()
            targetFile.createOverWriter().use { overWriter ->
                val cursors = filesToMerge.map { it.cursor() }
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
                    val basicIterator = commands.checkOrdered(strict = false).orderedDistinct()

                    val finalIterator = if (retainOldVersions) {
                        basicIterator
                    } else {
                        // we have to drop old versions...
                        basicIterator.latestVersionOnly()
                            // ...and if the latest version happens to be a DELETE, we ignore the key.
                            .filter { it.opCode != Command.OpCode.DEL }
                    }

                    ChronoStoreFileWriter(
                        outputStream = overWriter.outputStream,
                        settings = this.newFileSettings,
                        metadata = emptyMap()
                    ).use { writer ->
                        writer.writeFile(numberOfMerges = maxMerge + 1, orderedCommands = finalIterator)
                    }
                } finally {
                    for (cursor in cursors) {
                        cursor.close()
                    }
                }

                this.lock.write {
                    overWriter.commit()
                    this.garbageFileManager.addAll(filesToMerge.map { it.virtualFile.name })
                    this.fileList.removeAll(filesToMerge)
                }
            }
        }
    }

    private fun getFilesToMerge(fileIndices: Set<Int>): MutableList<LSMTreeFile> {
        val filesToMerge = mutableListOf<LSMTreeFile>()
        var previousFile: LSMTreeFile? = null
        for (file in this.fileList) {
            if (filesToMerge.isNotEmpty() && previousFile != null && previousFile.index !in fileIndices && file.index in fileIndices) {
                // files are not adjacent to each other (in terms of stored versions). That's bad, we must prevent this,
                // otherwise we cannot establish a chronological ordering for our files anymore.
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
        return filesToMerge
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
        } catch (e: Throwable) {
            log.error(e) { "Exception during async write task: ${e}" }
            throw e
        } finally {
            this.activeTaskLock.write {
                this.activeTaskMonitor = null
                this.activeTaskCondition.signalAll()
            }
        }
    }

    private fun createFileNameForIndex(fileIndex: Int): String {
        require(fileIndex >= 0) { "Argument 'fileIndex' (${fileIndex}) must not be negative!" }
        // The maximum file index has 10 digits, so we left-pad with zeroes.
        // This is a small overhead which effectively shouldn't matter, but
        // we gain the somewhat useful property that the lexicographic sort
        // of the files is equivalent to the ascending numeric index sort.
        // In other words: you can sort the files in the file explorer and
        // will get the proper order.
        return fileIndex.toString().padStart(10, '0') + FILE_EXTENSION
    }
}
