package org.chronos.chronostore.lsm

import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.compaction.LeveledCompactionStrategy
import org.chronos.chronostore.api.compaction.TieredCompactionStrategy
import org.chronos.chronostore.api.exceptions.FileMissingException
import org.chronos.chronostore.api.exceptions.FlushException
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.forEach
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.mainTask
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTask
import org.chronos.chronostore.async.tasks.CompletableAsyncTask
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.format.ChronoStoreFileWriter
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.chronos.chronostore.lsm.LSMTreeFile.Companion.FILE_EXTENSION
import org.chronos.chronostore.lsm.cache.FileHeaderCache
import org.chronos.chronostore.lsm.cache.LocalBlockCache
import org.chronos.chronostore.lsm.compaction.algorithms.CompactionTrigger
import org.chronos.chronostore.lsm.compaction.tasks.FlushInMemoryTreeToDiskTask
import org.chronos.chronostore.lsm.compaction.tasks.SingleStoreMajorCompactionTask
import org.chronos.chronostore.lsm.compaction.tasks.SingleStoreMinorCompactionTask
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.manifest.operations.CompactionOperation
import org.chronos.chronostore.manifest.operations.FullCompactionOperation
import org.chronos.chronostore.manifest.operations.LeveledCompactionOperation
import org.chronos.chronostore.manifest.operations.TieredCompactionOperation
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTSN
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.GroupingExtensions.toSets
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.concurrent.TaskQueue
import org.chronos.chronostore.util.cursor.*
import org.chronos.chronostore.util.logging.LogExtensions.perfTrace
import org.chronos.chronostore.util.unit.BinarySize
import org.chronos.chronostore.util.unit.BinarySize.Companion.Bytes
import org.pcollections.TreePMap
import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.system.measureTimeMillis

class LSMTree(
    val storeId: StoreId,
    initialStoreMetadata: StoreMetadata,
    private val forest: LSMForestMemoryManager,
    compactionExecutor: Executor,
    memtableFlushExecutor: Executor,
    private val manifestFile: ManifestFile,
    private val directory: VirtualDirectory,
    private val blockCache: LocalBlockCache,
    private val fileHeaderCache: FileHeaderCache,
    private val driverFactory: RandomFileAccessDriverFactory,
    private val newFileSettings: ChronoStoreFileSettings,
    private val getSmallestOpenReadTSN: () -> TSN?,
) {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    @Volatile
    private var inMemoryTree = TreePMap.empty<KeyAndTSN, Command>()
    private val inMemoryTreeSizeInBytes = AtomicLong(0)
    private val lock = ReentrantReadWriteLock(true)
    private val treeSizeChangedCondition = this.lock.writeLock().newCondition()

    private val fileList: MutableList<LSMTreeFile>
    private val nextFreeFileIndex = AtomicInteger(0)

    private val cursorManager = CursorManager()
    private val garbageFileManager = GarbageFileManager()

    private val compactionTaskQueue = TaskQueue(compactionExecutor)
    private val flushTaskQueue = TaskQueue(memtableFlushExecutor)

    init {
        if (!this.directory.exists()) {
            this.directory.mkdirs()
        }
        loadFilesAndSyncWithStoreMetadata(initialStoreMetadata)
        val existingFiles = this.loadFileList()
        this.fileList = existingFiles
        this.nextFreeFileIndex.set(getNextFreeFileIndex(this.fileList))
        forest.addTree(this)
    }

    private fun loadFilesAndSyncWithStoreMetadata(initialStoreMetadata: StoreMetadata) {
        val existingFileIndices = this.loadFileIndices()
        val filesAccordingToManifest = initialStoreMetadata.lsmFiles.keys
        this.deleteGarbageFilesOnStoreCreation(existingFileIndices, filesAccordingToManifest)
        this.checkForMissingFiles(filesAccordingToManifest, existingFileIndices)
    }

    private fun checkForMissingFiles(
        filesAccordingToManifest: MutableSet<FileIndex>,
        existingFileIndices: Set<FileIndex>,
    ) {
        val missingFiles = filesAccordingToManifest.asSequence()
            .filter { it !in existingFileIndices }
            .map { this.createFileNameForIndex(it) }
            .toSet()

        if (missingFiles.isNotEmpty()) {
            throw FileMissingException("The following files are missing from store ${this.storeId}: ${missingFiles.joinToString()}")
        }
    }

    private fun deleteGarbageFilesOnStoreCreation(
        existingFileIndices: MutableSet<FileIndex>,
        filesAccordingToManifest: MutableSet<FileIndex>,
    ) {
        val fileIndexIterator = existingFileIndices.iterator()
        while (fileIndexIterator.hasNext()) {
            val existingFileIndex = fileIndexIterator.next()
            if (existingFileIndex in filesAccordingToManifest) {
                // this file is expected, keep it.
                continue
            }
            // this file is garbage and needs to be removed.
            val file = this.directory.file(this.createFileNameForIndex(existingFileIndex))
            file.deleteOverWriterFileIfExists()
            file.delete()
            fileIndexIterator.remove()
        }
    }

    private fun loadFileIndices(): MutableSet<FileIndex> {
        return directory.list()
            .asSequence()
            .map { it.substringAfterLast(File.separatorChar) }
            .filter { it.endsWith(FILE_EXTENSION) }
            .mapNotNull(::parseFileIndexOrNull)
            .toMutableSet()
    }

    private fun loadFileList(): MutableList<LSMTreeFile> {
        return directory.list().asSequence()
            .map { it.substringAfterLast(File.separatorChar) }
            .filter { it.endsWith(FILE_EXTENSION) }
            .mapNotNull(::createLsmTreeFileOrNull)
            // sort by oldest TSN ascending (i.e. latest data is at the end of the list)
            .sortedBy { it.header.metaData.minTSN }
            .toMutableList()
    }

    private fun parseFileIndexOrNull(fileName: String): Int? {
        return fileName
            .substringAfterLast(File.separatorChar)
            .removeSuffix(FILE_EXTENSION)
            .toIntOrNull()
    }

    private fun getNextFreeFileIndex(fileList: List<LSMTreeFile>): Int {
        val maxExistingFileIndex = fileList.maxOfOrNull { it.index }
            ?: return 0   // we don't have any files yet, start with 0

        // we already have existing files, take the next free index (existing + 1)
        return maxExistingFileIndex + 1
    }

    private fun createLsmTreeFileOrNull(name: String): LSMTreeFile? {
        return this.createLsmTreeFileOrNull(this.directory.file(name))
    }

    private fun createLsmTreeFile(file: VirtualFile): LSMTreeFile {
        return this.createLsmTreeFileOrNull(file)
            ?: throw IllegalArgumentException("Failed to parse index from file name: '${file.path}'!")
    }

    private fun createLsmTreeFileOrNull(file: VirtualFile): LSMTreeFile? {
        val index = this.parseFileIndexOrNull(file.name)
            ?: return null

        return LSMTreeFile(
            virtualFile = file,
            index = index,
            driverFactory = this.driverFactory,
            blockCache = this.blockCache,
            fileHeaderCache = this.fileHeaderCache,
        )
    }


    val inMemorySize: BinarySize
        get() = this.inMemoryTreeSizeInBytes.get().Bytes

    val path: String
        get() = this.directory.path

    val allFiles: List<LSMTreeFile>
        get() = this.lock.read { this.fileList.toList() }

    val latestReceivedCommitTSN: TSN?
        get() {
            this.lock.read {
                // first, check if we have commits in memory.
                val latestInMemoryCommitTSN = this.inMemoryTree.keys.maxOfOrNull { it.tsn }
                if (latestInMemoryCommitTSN != null) {
                    return latestInMemoryCommitTSN
                }
                // we have no in-memory changes, so we have
                // to check our latest persisted TSN
                return this.latestPersistedCommitTSN
            }
        }

    val latestPersistedCommitTSN: TSN?
        get() {
            this.lock.read {
                // we don't have in-memory commits, check the files.
                // IMPORTANT: we assume here that the highest TSN is in the file with the HIGHEST index!
                return this.fileList.lastOrNull()?.header?.metaData?.maxTSN
            }
        }

    val estimatedNumberOfEntries: Long
        get() {
            this.lock.read {
                return this.fileList.sumOf { it.header.metaData.headEntries }
            }
        }

    fun get(keyAndTSN: KeyAndTSN): Command? {
        this.lock.read {
            // first, check the in-memory tree
            val inMemoryEntry = this.inMemoryTree.floorEntry(keyAndTSN)
            if ((inMemoryEntry != null) && (inMemoryEntry.key.key == keyAndTSN.key)) {
                return inMemoryEntry.value
            }
            // then, search the on-disk files (in reverse order, latest first)
            for (lsmTreeFile in this.fileList.asReversed()) {
                val result = lsmTreeFile.get(keyAndTSN)
                if (result != null) {
                    return result
                }
            }
            return null
        }
    }

    fun openCursor(transaction: ChronoStoreTransaction): Cursor<Bytes, Command> {
        val tsn = transaction.lastVisibleSerialNumber
        this.lock.read {
            val inMemoryDataMap = this.inMemoryTree
            val inMemoryCursor = if (inMemoryDataMap.isEmpty()) {
                EmptyCursor { "In-Memory Cursor" }
            } else {
                NavigableMapCursor(inMemoryDataMap)
            }
            val fileCursors = this.fileList.asSequence().map { this.cursorManager.openCursorOn(transaction, it) }.toMutableList()
            if (fileCursors.isEmpty()) {
                return VersioningCursor(inMemoryCursor, tsn, includeDeletions = true)
            }
            if (inMemoryCursor !is EmptyCursor) {
                fileCursors.add(inMemoryCursor)
            }

            val versioningCursors = fileCursors.map { VersioningCursor(it, tsn, includeDeletions = true) }

            // we need to create a single cursor across all versioning cursors.
            // A single overlay cursor takes one cursor and "overlays" the result of another on top of it
            // (i.e. the second cursor has more recent information than the first and should override it,
            // if they provide different data we keep both).
            // The cursors we are dealing with here are already in the correct order (oldest to newest).
            // If we simply take a list of cursors A,B,C,D and overlay them one after another, we create
            // a structure like this:
            //
            //                   O
            //                  / \
            //                 A   O
            //                    / \
            //                   B   O
            //                      / \
            //                     C   D
            //
            // While this produces the correct result, it is easy to see that it degenerates into a list.
            // This is not very efficient. The better solution is to create a balanced tree, like so:
            //
            //                   O
            //                /     \
            //               O       O
            //              / \     / \
            //             A   B   C   D
            //
            // This means that we balance out the comparisons and reduce them. This delivers the same
            // result, is much more efficient and can cut the time for a full iteration in half.
            fun buildOverlayCursorTree(cursors: List<Cursor<Bytes, Command>>): Cursor<Bytes, Command> {
                return when (cursors.size) {
                    0 -> throw IllegalArgumentException("List of cursors must not be empty!")
                    1 -> cursors.first()
                    2 -> OverlayCursor(cursors[0], cursors[1])
                    else -> {
                        // find the half-way point
                        val halfWay = cursors.lastIndex / 2
                        val leftCursor = buildOverlayCursorTree(cursors.subList(0, halfWay))
                        val rightCursor = buildOverlayCursorTree(cursors.subList(halfWay, cursors.size))
                        return OverlayCursor(leftCursor, rightCursor)
                    }
                }
            }

            return buildOverlayCursorTree(versioningCursors)
        }
    }

    fun putAll(commands: Iterable<Command>) {
        this.lock.write {
            val containedEntry = commands.asSequence().map { it.keyAndTSN }.firstOrNull(this.inMemoryTree::containsKey)
            require(containedEntry == null) {
                "The key-and-tsn ${containedEntry} is already contained in in-memory LSM tree '${this.storeId}'!"
            }

            val keyAndTSNToCommand = commands.associateBy { it.keyAndTSN }
            val bytesToInsert = keyAndTSNToCommand.values.sumOf { it.byteSize }.toLong()

            // if the tree size gets too big, we have to block the writes until we have enough space
            // again. This is very unfortunate, but cannot be avoided. If the writer generates more
            // data per second than the flush task can write to disk, the only other alternative is
            // to claim more and more RAM until we eventually run into an OutOfMemoryError.
            this.waitUntilInMemoryTreeHasCapacity(bytesToInsert)

            // [C0001] it is very important that we add ALL the data from the commands iterable to
            // the tree at once. If we don't do that, we may risk flushing partial (!!) transaction
            // data to disk which absolutely has to be avoided; otherwise efficient startup recovery
            // is not possible. By updating the "this.inMemoryTree" reference only with the fully
            // updated tree, we guarantee that each flush task either reads all commands, or none.
            this.inMemoryTree = this.inMemoryTree.plusAll(keyAndTSNToCommand)
            this.inMemoryTreeSizeInBytes.getAndAdd(bytesToInsert)

            // the in-memory tree size has changed, notify listeners
            this.treeSizeChangedCondition.signalAll()
        }
    }

    private fun waitUntilInMemoryTreeHasCapacity(bytesToInsert: Long) {
        this.forest.onBeforeInMemoryInsert(this, bytesToInsert)
    }

    fun getMaxPersistedTSN(): TSN {
        return fileList.asSequence()
            .mapNotNull { it.header.metaData.maxTSN }
            .maxOrNull() ?: -1
    }

    fun scheduleMajorCompaction(): CompletableFuture<*> {
        // when we do a major compaction anyway, all open minor compaction tasks become useless
        // because their results will be superseded by the major compaction anyway. Remove them from the queue.
        this.compactionTaskQueue.cancelWaitingTasksIf { it is SingleStoreMinorCompactionTask }
        return this.compactionTaskQueue.schedule(SingleStoreMajorCompactionTask(this, this.manifestFile))
    }

    fun scheduleMinorCompaction(): CompletableFuture<*> {
        return this.compactionTaskQueue.schedule(SingleStoreMinorCompactionTask(this))
    }

    fun scheduleMemtableFlush(): CompletableFuture<*> {
        val completableTask = CompletableAsyncTask(FlushInMemoryTreeToDiskTask(this))
        this.flushTaskQueue.schedule(completableTask)
        return completableTask.future
    }

    fun flushInMemoryDataToDisk(minFlushSize: BinarySize, monitor: TaskMonitor): FlushResult? {
        log.perfTrace { "TASK: Flush in-memory data to disk" }
        try {
            val timeBefore = System.currentTimeMillis()
            monitor.reportStarted("Flushing Data to Disk")
            if (this.inMemoryTree.isEmpty() || this.inMemorySize < minFlushSize) {
                // flush not necessary
                monitor.reportDone()
                return null
            }
            log.perfTrace { "Flushing LSM Tree '${this.directory}'!" }
            val commands = monitor.subTask(0.1, "Collecting Entries to flush") {
                // [C0001]: We *must* take *all* of the commits in the in-memory tree
                // and flush them to avoid writing SST files with partial transactions in them.
                this.inMemoryTree
            }
            val newFileIndex = this.nextFreeFileIndex.getAndIncrement()
            log.perfTrace { "Target file index ${newFileIndex} will be used for flush of tree ${this.path}" }
            val file = this.directory.file(this.createFileNameForIndex(newFileIndex))
            val flushTime = measureTimeMillis {
                monitor.subTask(0.8, "Writing File") {
                    file.deleteOverWriterFileIfExists()
                    file.withOverWriter { overWriter ->
                        ChronoStoreFileWriter(
                            outputStream = overWriter.outputStream,
                            settings = this.newFileSettings,
                        ).use { writer ->
                            log.perfTrace { "Flushing ${commands.size} commands from in-memory segment into '${file.path}'." }
                            writer.writeFile(
                                // we're flushing this file from in-memory,
                                // so the resulting file has never been merged.
                                numberOfMerges = 0,
                                orderedCommands = commands.values.iterator(),
                                commandCountEstimate = commands.size.toLong(),
                            )
                        }
                        overWriter.commit()
                    }
                }
            }
            this.manifestFile.appendFlushOperation(this.storeId, newFileIndex)

            log.perfTrace { "Flush into index file ${newFileIndex} completed in ${flushTime}ms. Redirecting traffic to new data file for LSM tree ${this.path}" }
            monitor.subTask(0.1, "Redirecting traffic to file") {
                this.lock.write {
                    this.fileList += this.createLsmTreeFile(file)
                    // ensure that the merged file is at the correct position in the list
                    this.fileList.sortBy { it.header.metaData.minTSN }
                    log.perfTrace { "Removing ${commands.keys.size} keys from the in-memory tree (which has ${this.inMemoryTree.size} keys)..." }
                    this.inMemoryTree = this.inMemoryTree.minusAll(commands.keys)
                    log.perfTrace { "${inMemoryTree.size} entries remaining in-memory after flush of tree ${this.path}" }
                    this.inMemoryTreeSizeInBytes.addAndGet(commands.values.sumOf { it.byteSize } * -1L)

                    // the in-memory tree size has changed, notify listeners
                    this.forest.onInMemoryFlush(this)
                }
            }

            val timeAfter = System.currentTimeMillis()

            monitor.reportDone()
            return FlushResult(
                targetFile = file,
                targetFileIndex = newFileIndex,
                bytesWritten = file.length,
                entriesWritten = commands.size,
                runtimeMillis = timeAfter - timeBefore,
            )
        } catch (e: Exception) {
            val ex = FlushException("An exception occurred during flush task on '${storeId}': ${e}", e)
            monitor.reportFailed(ex)
            throw ex
        }
    }

    fun performGarbageCollection(taskMonitor: TaskMonitor) = taskMonitor.mainTask("Collecting Garbage in '${this.path}'") {
        val deleted = mutableSetOf<String>()
        taskMonitor.forEach(1.0, "Deleting old files", this.garbageFileManager.garbageFiles.toList()) { fileName ->
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
    }

    fun mergeFiles(fileIndices: Set<Int>, keepTombstones: Boolean, trigger: CompactionTrigger, monitor: TaskMonitor): Set<FileIndex> {
        log.debug { "TASK [${this.storeId}] merge files due to trigger ${trigger}: ${fileIndices.sorted().joinToString(prefix = "[", separator = ", ", postfix = "]")}" }
        monitor.mainTask("Merging ${fileIndices.size} files") {
            val timeBefore = System.currentTimeMillis()
            val filesToMerge = this.getFilesToMerge(fileIndices)

            if (filesToMerge.size < 2) {
                return emptySet()
            }
            // get the file index we're going to use (nobody else will get the same index, because of the AtomicInteger used here)
            val targetFileIndex = this.nextFreeFileIndex.getAndIncrement()

            // this is the file we're going to write to
            val targetFile = this.directory.file(this.createFileNameForIndex(targetFileIndex))
            val maxNumberOfMergesInInputFiles = filesToMerge.maxOfOrNull {
                it.header.metaData.numberOfMerges
            } ?: 0

            // TODO [LOGIC]: Currently we only create a single output file per compaction. We want to split that up (e.g. using Spooky, max. file size, etc.)
            targetFile.deleteOverWriterFileIfExists()
            targetFile.createOverWriter().use { overWriter ->
                val totalEntries = filesToMerge.sumOf { it.header.metaData.totalEntries }
                val cursors = filesToMerge.map { it.cursor() }
                try {
                    ChronoStoreFileWriter(
                        outputStream = overWriter.outputStream,
                        settings = this.newFileSettings,
                    ).use { writer ->
                        CompactionUtil.compact(
                            cursors = cursors,
                            writer = writer,
                            maxNumberOfMergesInInputFiles = maxNumberOfMergesInInputFiles,
                            resultingCommandCountEstimate = totalEntries,
                            keepTombstones = keepTombstones,
                            smallestReadTSN = this.getSmallestOpenReadTSN(),
                        )
                    }
                } finally {
                    for (cursor in cursors) {
                        cursor.close()
                    }
                }

                val targetFiles = setOf(targetFile)

                // prepare the new LSM file outside the lock (we don't need the lock for that,
                // and opening the file can take a few seconds)
                val mergedLsmTreeFiles = targetFiles.asSequence()
                    .map { this.createLsmTreeFile(it) }
                    .toSet()

                this.lock.write {
                    overWriter.commit()
                    this.manifestFile.appendOperation { operationSequenceNumber ->
                        createManifestOperationForCompaction(
                            trigger = trigger,
                            filesToMerge = filesToMerge,
                            operationSequenceNumber = operationSequenceNumber,
                            mergedLsmTreeFiles = mergedLsmTreeFiles,
                            fileIndices = fileIndices,
                        )
                    }

                    this.garbageFileManager.addAll(filesToMerge.map { it.virtualFile.name })
                    this.fileList.removeAll(filesToMerge.toSet())
                    this.fileList.addAll(mergedLsmTreeFiles)
                }

                // perform garbage collection in an attempt to free disk space
                this.performGarbageCollection(TaskMonitor.create())

                val timeAfter = System.currentTimeMillis()
                log.debug { "TASK [${this.storeId}] merge files due to trigger ${trigger} completed in ${timeAfter - timeBefore}ms." }
                return mergedLsmTreeFiles.asSequence().map { it.index }.toSet()
            }
        }
    }

    private fun createManifestOperationForCompaction(
        trigger: CompactionTrigger,
        filesToMerge: Set<LSMTreeFile>,
        operationSequenceNumber: Int,
        mergedLsmTreeFiles: Set<LSMTreeFile>,
        fileIndices: Set<Int>,
    ): CompactionOperation {
        return when (trigger) {
            CompactionTrigger.TIER_SPACE_AMPLIFICATION,
            CompactionTrigger.TIER_SIZE_RATIO,
            CompactionTrigger.TIER_HEIGHT_REDUCTION,
                -> createTieredCompactionManifestOperation(
                filesToMerge = filesToMerge,
                operationSequenceNumber = operationSequenceNumber,
                mergedLsmTreeFiles = mergedLsmTreeFiles,
            )

            CompactionTrigger.LEVELED_LEVEL0,
            CompactionTrigger.LEVELED_TARGET_SIZE_RATIO,
                -> createLeveledCompactionManifestOperation(
                filesToMerge = filesToMerge,
                operationSequenceNumber = operationSequenceNumber,
                mergedLsmTreeFiles = mergedLsmTreeFiles,
            )

            CompactionTrigger.FULL_COMPACTION -> createFullCompactionManifestOperation(
                operationSequenceNumber = operationSequenceNumber,
                fileIndices = fileIndices,
                mergedLsmTreeFiles = mergedLsmTreeFiles,
            )
        }
    }

    private fun createTieredCompactionManifestOperation(
        filesToMerge: Set<LSMTreeFile>,
        operationSequenceNumber: Int,
        mergedLsmTreeFiles: Set<LSMTreeFile>,
    ): TieredCompactionOperation {
        val storeMetadata = this.getStoreMetadata()
        check(storeMetadata.compactionStrategy is TieredCompactionStrategy) {
            "Cannot apply a tiered compaction operation on store '${this.storeId}' because this store is configured to use leveled compaction!"
        }
        val fileIndexToFileInfo = storeMetadata.getAllFileInfos().associateBy { it.fileIndex }
        val tierToFileIndices = filesToMerge.asSequence()
            .mapNotNull { fileIndexToFileInfo[it.index] }
            .groupingBy { it.levelOrTier }
            .toSets { it.fileIndex }
        val outputFileIndices = mergedLsmTreeFiles.asSequence()
            .map { it.index }
            .toSet()
        return TieredCompactionOperation(
            sequenceNumber = operationSequenceNumber,
            storeId = this.storeId,
            outputFileIndices = outputFileIndices,
            tierToFileIndices = tierToFileIndices,
        )
    }

    private fun createLeveledCompactionManifestOperation(
        filesToMerge: Set<LSMTreeFile>,
        operationSequenceNumber: Int,
        mergedLsmTreeFiles: Set<LSMTreeFile>,
    ): LeveledCompactionOperation {
        val storeMetadata = this.getStoreMetadata()
        check(storeMetadata.compactionStrategy is LeveledCompactionStrategy) {
            "Cannot apply a tiered compaction operation on store '${this.storeId}' because this store is configured to use leveled compaction!"
        }
        val fileIndexToFileInfo = storeMetadata.getAllFileInfos().associateBy { it.fileIndex }
        val inputFileInfos = filesToMerge.mapNotNull { fileIndexToFileInfo[it.index] }

        val levelToInputFiles = inputFileInfos.groupingBy { it.levelOrTier }.toSets { it.fileIndex }
        // there should be exactly 2 levels in the map:
        // - the lower level, from which we merge (potentially level 0)
        // - the upper level which receives the merge (potentially the highest level

        check(levelToInputFiles.size == 2) {
            "Expected exactly two levels to be affected by a minor compaction, but got ${levelToInputFiles.keys.size}: ${levelToInputFiles.keys}"
        }

        val lowerLevelIndex = levelToInputFiles.keys.min()
        val upperLevelIndex = levelToInputFiles.keys.max()
        val lowerLevelFileIndices = levelToInputFiles.getValue(lowerLevelIndex)
        val upperLevelFileIndices = levelToInputFiles.getValue(upperLevelIndex)
        val outputFileIndices = mergedLsmTreeFiles.asSequence()
            .map { it.index }
            .toSet()
        return LeveledCompactionOperation(
            sequenceNumber = operationSequenceNumber,
            storeId = this.storeId,
            outputFileIndices = outputFileIndices,
            upperLevelIndex = upperLevelIndex,
            upperLevelFileIndices = upperLevelFileIndices,
            lowerLevelFileIndices = lowerLevelFileIndices,
            lowerLevelIndex = lowerLevelIndex,
        )
    }

    private fun createFullCompactionManifestOperation(
        operationSequenceNumber: Int,
        fileIndices: Set<Int>,
        mergedLsmTreeFiles: Set<LSMTreeFile>,
    ): FullCompactionOperation {
        val storeMetadata = this.getStoreMetadata()
        val maxLevelOrTierIndex = when (val compactionStrategy = storeMetadata.compactionStrategy) {
            is LeveledCompactionStrategy -> compactionStrategy.maxLevels - 1
            is TieredCompactionStrategy -> compactionStrategy.numberOfTiers - 1
        }
        val outputFileIndices = mergedLsmTreeFiles.asSequence()
            .map { it.index }
            .toSet()
        return FullCompactionOperation(
            sequenceNumber = operationSequenceNumber,
            storeId = this.storeId,
            inputFileIndices = fileIndices,
            outputFileIndices = outputFileIndices,
            outputLevelOrTier = maxLevelOrTierIndex,
        )
    }

    fun getStoreMetadata(): StoreMetadata {
        val manifest = this.manifestFile.getManifest()
        val storeMetadata = manifest.getStore(this.storeId)
        return storeMetadata
    }

    private fun getFilesToMerge(fileIndices: Set<Int>): Set<LSMTreeFile> {
        val filesToMerge = mutableSetOf<LSMTreeFile>()
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

    override fun toString(): String {
        return "LSMTree[${this.storeId}]"
    }
}
