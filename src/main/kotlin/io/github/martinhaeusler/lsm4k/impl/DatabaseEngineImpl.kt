package io.github.martinhaeusler.lsm4k.impl

import io.github.martinhaeusler.lsm4k.api.*
import io.github.martinhaeusler.lsm4k.api.statistics.StatisticsManager
import io.github.martinhaeusler.lsm4k.async.executor.AsyncTaskManagerImpl
import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor
import io.github.martinhaeusler.lsm4k.impl.store.StoreImpl
import io.github.martinhaeusler.lsm4k.io.format.BlockLoader
import io.github.martinhaeusler.lsm4k.io.format.CompressionAlgorithm
import io.github.martinhaeusler.lsm4k.io.format.FileHeader
import io.github.martinhaeusler.lsm4k.io.format.LSMFileSettings
import io.github.martinhaeusler.lsm4k.io.prefetcher.BlockPrefetchingManager
import io.github.martinhaeusler.lsm4k.io.structure.LSM4KStructure
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFileSystem
import io.github.martinhaeusler.lsm4k.lsm.LSMForestMemoryManager
import io.github.martinhaeusler.lsm4k.lsm.LSMTree
import io.github.martinhaeusler.lsm4k.lsm.cache.BlockCache
import io.github.martinhaeusler.lsm4k.lsm.cache.FileHeaderCache
import io.github.martinhaeusler.lsm4k.lsm.cache.NoBlockCache
import io.github.martinhaeusler.lsm4k.lsm.compaction.tasks.CheckpointTask
import io.github.martinhaeusler.lsm4k.lsm.compaction.tasks.TriggerMajorCompactionOnAllStoresTask
import io.github.martinhaeusler.lsm4k.lsm.compaction.tasks.TriggerMinorCompactionOnAllStoresTask
import io.github.martinhaeusler.lsm4k.lsm.garbagecollector.tasks.GarbageCollectorTask
import io.github.martinhaeusler.lsm4k.manifest.Manifest
import io.github.martinhaeusler.lsm4k.manifest.ManifestFile
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.util.report.DatabaseStructureReport
import io.github.martinhaeusler.lsm4k.util.sequence.SequenceExtensions.toTreeMap
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsCollector
import io.github.martinhaeusler.lsm4k.wal.WALReadBuffer
import io.github.martinhaeusler.lsm4k.wal.WriteAheadLog
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import kotlin.math.max
import kotlin.time.Duration

class DatabaseEngineImpl(
    private val vfs: VirtualFileSystem,
    private val configuration: LSM4KConfiguration,
) : DatabaseEngine {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    private val statisticsCollector = StatisticsCollector()

    /** Tells if the engine is currently open and ready for requests. */
    @Volatile
    private var isOpen = true

    /** The state of the entire engine. */
    @Volatile
    private var state: EngineState = EngineState.STARTING

    /**
     * Manages the lock file which ensures that only one process in the operating system accesses the engine directory.
     */
    private val lockFileManager = LockFileManager(
        vfs.file(LSM4KStructure.LOCK_FILE_NAME)
    )

    /**
     * The [Killswitch] is a central point which allows to immediately shut down the engine in case of critical errors.
     */
    private val killswitch = Killswitch(this::panic)

    /**
     * The manifest file reads and writes the [Manifest] and caches the latest version.
     */
    private val manifestFile = ManifestFile(this.vfs.file(ManifestFile.FILE_NAME))

    /**
     * Manages the latest [TSN].
     */
    private val tsnManager: TSNManager

    /**
     * Caches [FileHeader] instances for quick access.
     */
    private val fileHeaderCache = FileHeaderCache.create(
        fileHeaderCacheSize = configuration.fileHeaderCacheSize,
        statisticsReporter = this.statisticsCollector
    )

    /**
     * The prefetching manager to use for block loading operations.
     *
     * May be `null` if prefetching is disabled.
     *
     * Needs to be closed explicitly on shutdown (may contain a thread pool).
     */
    private val prefetchingManager: BlockPrefetchingManager? = BlockPrefetchingManager.createIfNecessary(
        fileHeaderCache = this.fileHeaderCache,
        driverFactory = this.configuration.randomFileAccessDriverFactory,
        prefetchingThreads = this.configuration.prefetchingThreads,
        statisticsReporter = this.statisticsCollector,
    )

    /**
     * The [BlockLoader] to be used by the engine.
     *
     * The block loader fetches blocks from disk. Some block loaders also support asynchronous loading.
     */
    private val blockLoader: BlockLoader = this.prefetchingManager
        ?: BlockLoader.basic(
            driverFactory = this.configuration.randomFileAccessDriverFactory,
            headerCache = this.fileHeaderCache,
            statisticsReporter = this.statisticsCollector,
        )

    /**
     * The [BlockCache] contains the data blocks which have been loaded for future reference.
     *
     * Notably, the [BlockCache] implements the [BlockLoader] interface itself. It therefore
     * qualifies as a "loading cache": if a cache miss would occur, the loader is triggered
     * instead.
     *
     * If the [LSM4KConfiguration.blockCacheSize] is set to a non-positive value,
     * an instance of [NoBlockCache] is used instead, which implements the same interface
     * but does not cache any of the results.
     */
    private val blockCache: BlockCache = BlockCache.create(
        maxSize = this.configuration.blockCacheSize,
        loader = this.blockLoader,
        statisticsReporter = this.statisticsCollector,
    )

    /**
     * Memory manager for the in-memory portions of [LSMTree]s (a.k.a. "memtables").
     *
     * Balances memory between trees and flushes data to disk as necessary.
     */
    val forest: LSMForestMemoryManager = LSMForestMemoryManager(
        maxForestSize = this.configuration.maxForestSize.bytes,
        flushThresholdSize = (this.configuration.maxForestSize.bytes * this.configuration.forestFlushThreshold).toLong(),
        statisticsReporter = this.statisticsCollector,
    )

    /**
     * Manages the different [Store]s. Allows to add, remove, list and retrieve stores.
     */
    private val storeManager: StoreManagerImpl = StoreManagerImpl(
        vfs = this.vfs,
        blockCache = this.blockCache,
        fileHeaderCache = this.fileHeaderCache,
        forest = this.forest,
        driverFactory = this.configuration.randomFileAccessDriverFactory,
        newFileSettings = LSMFileSettings(
            compression = CompressionAlgorithm.forCompressorName(configuration.compressionAlgorithm),
            maxBlockSize = configuration.maxBlockSize,
        ),
        manifestFile = this.manifestFile,
        configuration = this.configuration,
        getSmallestOpenReadTSN = { this.transactionManager.getSmallestOpenReadTSN() },
        killswitch = this.killswitch,
        statisticsReporter = this.statisticsCollector,
    )

    /**
     * General-purpose asynchronous task manager with support for scheduling and recurrence.
     *
     * Used e.g. for scheduling the checkpoint task, the compaction tasks and the garbage collector task.
     */
    private val asyncTaskManager = AsyncTaskManagerImpl(
        executorService = Executors.newSingleThreadScheduledExecutor(),
        getEngineState = this::state,
    )

    /** The Write-Ahead-Log (WAL) of the engine. Manages all WAL files. */
    private val writeAheadLog: WriteAheadLog

    /** Opens new transactions, tracks them, and closes them upon request. */
    private val transactionManager: TransactionManager

    /**
     * An async task which creates a checkpoint.
     *
     * A successfully created checkpoint helps to improve the next startup time.
     */
    private val checkpointTask: CheckpointTask

    init {
        val timeBefore = System.currentTimeMillis()
        // lock the directory to prevent multiple processes to access the same directory.
        this.lockFileManager.lock()

        val systemDir = this.vfs.directory(SystemStore.PATH_PREFIX)
        val walDirectory = systemDir.directory(LSM4KStructure.WRITE_AHEAD_LOG_DIR_NAME)
        val isEmptyDatabase = !walDirectory.exists()
        if (isEmptyDatabase) {
            // If there is no WAL directory, we're going to set up a completely new database.
            // To avoid any trouble with existing files, we demand that our working directory
            // must be empty.
            if (this.vfs.listRootLevelElements().any { it.name != LSM4KStructure.LOCK_FILE_NAME }) {
                throw IllegalArgumentException("Cannot create new database in '${vfs.rootPath}': the directory is not empty!")
            }
        }

        systemDir.mkdirs()

        this.writeAheadLog = WriteAheadLog(
            directory = walDirectory,
            maxWalFileSizeBytes = this.configuration.maxWriteAheadLogFileSize.bytes,
            minNumberOfFiles = this.configuration.minNumberOfWriteAheadLogFiles,
        )
        val currentTSN = if (isEmptyDatabase) {
            // The WAL file doesn't exist. It's a new, empty database.
            // We don't need a recovery, but we have to "set up camp".
            this.createNewEmptyDatabase()
        } else {
            // the WAL file exists, perform startup recovery.
            this.performStartupRecovery()
        }

        this.tsnManager = TSNManager(currentTSN)

        this.transactionManager = TransactionManager(
            storeManager = this.storeManager,
            tsnManager = this.tsnManager,
            writeAheadLog = this.writeAheadLog,
            statisticsReporter = this.statisticsCollector,
        )

        this.checkpointTask = CheckpointTask(
            writeAheadLog = this.writeAheadLog,
            storeManager = this.storeManager,
            manifestFile = this.manifestFile,
            killswitch = this.killswitch,
        )

        val checkpointCron = this.configuration.checkpointCron
        if (checkpointCron != null) {
            this.asyncTaskManager.scheduleRecurringWithCron(checkpointTask, checkpointCron)
        }

        val garbageCollectorTask = GarbageCollectorTask(storeManager)
        val garbageCollectionCron = this.configuration.garbageCollectionCron
        if (garbageCollectionCron != null) {
            this.asyncTaskManager.scheduleRecurringWithCron(garbageCollectorTask, garbageCollectionCron)
        }

        val minorCompactionCron = this.configuration.minorCompactionCron
        if (minorCompactionCron != null) {
            val triggerMinorCompactionOnAllStoresTask = TriggerMinorCompactionOnAllStoresTask(
                getAllStores = this.storeManager::getAllStoresInternal,
                killswitch = this.killswitch,
            )
            this.asyncTaskManager.scheduleRecurringWithCron(triggerMinorCompactionOnAllStoresTask, minorCompactionCron)
        } else {
            this.warnAboutMinorCompactionDisabled()
        }

        val majorCompactionCron = this.configuration.majorCompactionCron
        if (majorCompactionCron != null) {
            val triggerMajorCompactionOnAllStoresTask = TriggerMajorCompactionOnAllStoresTask(
                getAllStores = this.storeManager::getAllStoresInternal,
                killswitch = killswitch,
            )
            this.asyncTaskManager.scheduleRecurringWithCron(triggerMajorCompactionOnAllStoresTask, majorCompactionCron)
        } else {
            this.warnAboutMajorCompactionDisabled()
        }

        this.state = EngineState.RUNNING
        val timeAfter = System.currentTimeMillis()
        log.info { "Database Engine started in ${timeAfter - timeBefore}ms at ${this.rootPath}" }
    }

    private fun createNewEmptyDatabase(): TSN {
        log.info { "Creating a new, empty database in: ${this.vfs}" }
        this.storeManager.initialize(isEmptyDatabase = true)
        return 1L
    }

    private fun performStartupRecovery(): TSN {
        log.info { "Opening database in: ${this.vfs}" }
        // initialize the store manager so that we can read from it.
        this.storeManager.initialize(isEmptyDatabase = false)

        // check which is the HIGHEST TSN ever received by any store?
        // TODO [PERFORMANCE]: Fetching the metadata of EVERY LSM file in the store may become expensive
        val highWatermarkTSN = this.storeManager.getHighWatermarkTSN() ?: -1

        // remove any incomplete transactions from the WAL file. Ensure that
        // we still have access to the WAL file which produced the highest
        // TSN in the LSM trees after the operation has completed.
        this.writeAheadLog.performStartupRecoveryCleanup(highWatermarkTSN)

        val allStores = this.storeManager.getAllStoresInternal()
        // with the store info, replay the changes found in the WAL.
        return this.replayWriteAheadLogChanges(allStores)
    }

    private fun replayWriteAheadLogChanges(allStores: List<Store>): TSN {
        log.info { "Replaying Write-Ahead-Log" }
        // replay the entries in the WAL which have not been persisted yet
        var maxTSN = 0L
        val storeIdToLowWatermark = allStores.associate { it.storeId to it.lowWatermarkTSN }
        val walReadBuffer = WALReadBuffer(this.configuration.walBufferSize.bytes, storeIdToLowWatermark)
        val lsmTreesById = allStores.associate { it.storeId to (it as StoreImpl).tree }
        val timeBefore = System.currentTimeMillis()

        this.writeAheadLog.readWalStreaming(walReadBuffer) {
            // buffer needs to be flushed
            val modifiedStoreIds = walReadBuffer.getModifiedStoreIds()
            for (modifiedStoreId in modifiedStoreIds) {
                val lsmTree = lsmTreesById[modifiedStoreId]
                    ?: continue  // the store doesn't exist anymore, skip
                val commandsToFlush = walReadBuffer.getCommandsForStore(modifiedStoreId)
                val completedTSN = walReadBuffer.getCompletedTSNForStore(modifiedStoreId)

                if (commandsToFlush.isNotEmpty()) {
                    // we're missing the changes from this transaction,
                    // put them into the store.
                    lsmTree.putAll(commandsToFlush)
                }
                if (completedTSN != null) {
                    maxTSN = max(maxTSN, completedTSN)
                    lsmTree.setHighestCompletelyWrittenTSN(completedTSN)
                }
            }
        }

        val timeAfter = System.currentTimeMillis()
        log.info { "Successfully replayed transactions in the Write-Ahead-Log in ${timeAfter - timeBefore}ms." }
        return maxTSN
    }

    override fun beginReadOnlyTransaction(): LSM4KTransaction {
        return this.transactionManager.createNewReadOnlyTransaction()
    }

    override fun beginReadWriteTransaction(): LSM4KTransaction {
        return this.transactionManager.createNewReadWriteTransaction(this.configuration.defaultLockAcquisitionTimeout)
    }

    override fun beginReadWriteTransaction(lockAcquisitionTimeout: Duration?): LSM4KTransaction {
        return this.transactionManager.createNewReadWriteTransaction(lockAcquisitionTimeout)
    }

    override fun beginExclusiveTransaction(): LSM4KTransaction {
        return this.transactionManager.createNewExclusiveTransaction(this.configuration.defaultLockAcquisitionTimeout)
    }

    override fun beginExclusiveTransaction(lockAcquisitionTimeout: Duration?): LSM4KTransaction {
        return this.transactionManager.createNewExclusiveTransaction(lockAcquisitionTimeout)
    }

    override val rootPath: String
        get() = this.vfs.rootPath

    override fun close() {
        if (!isOpen) {
            return
        }
        isOpen = false
        log.info { "Shutting down Database Engine at '${this.vfs}'..." }
        this.state = EngineState.SHUTTING_DOWN
        // to ensure a quick startup recovery, ensure that we have a recent checkpoint.
        // If this operation is aborted by process kill, nothing bad happens except
        // that startup recovery may take a bit longer.
        if (this.configuration.checkpointOnShutdown) {
            this.checkpointTask.run(TaskMonitor.create())
        }

        // we're entering shutdown mode, so turn off the killswitch. There's no need
        // to kill anything during the shutdown process anymore.
        this.killswitch.disable()
        // close the transaction manager, this will roll back all active transactions
        // and prevent the creation of new ones.
        this.transactionManager.close()
        // close the write-ahead-log manager. This will cancel any ongoing writes.
        this.writeAheadLog.close()
        // close the async task manager. This will also shut down any concurrent tasks.
        this.asyncTaskManager.close()
        this.storeManager.close()
        this.prefetchingManager?.close()
        // release the file-based lock to allow another instance to be opened on
        // our target directory.
        this.lockFileManager.unlock()
        log.info { "Completed shutdown of Database Engine at '${this.vfs}'." }
    }

    private fun panic(message: String, cause: Throwable?) {
        log.error(cause) { "Database Engine Panic: A fatal failure has occurred during an operation. Store will shut down immediately in order to protect data integrity. Message: '${message}', cause: ${cause}" }
        this.state = EngineState.PANIC
        this.isOpen = false
        this.transactionManager.closePanic()
        this.writeAheadLog.closePanic()
        this.asyncTaskManager.closePanic()
        this.storeManager.closePanic()
        this.prefetchingManager?.closePanic()
        this.lockFileManager.unlockSafe()
        log.info(cause) { "Databse Engine has been shut down due to panic trigger." }
    }

    // =================================================================================================================
    // STATUS & STATISTICS REPORTING
    // Controls collection of statistics and generates reports on them.
    // =================================================================================================================

    override fun statusReport(): DatabaseStructureReport {
        val allLsmTrees = this.storeManager.getAllLsmTreesAdmin()
        return DatabaseStructureReport(
            rootPath = this.rootPath,
            storeReports = allLsmTrees.asSequence()
                .map { it.storeId to it.report() }
                .toTreeMap(),
            walReport = this.writeAheadLog.report(),
            currentTSN = this.tsnManager.getLastReturnedTSN(),
            maxPersistedTSN = allLsmTrees.asSequence()
                .mapNotNull { it.getMaxPersistedTSN() }
                .maxOrNull(),
            transactionReport = this.transactionManager.report()
        )
    }

    override val statistics: StatisticsManager
        get() = this.statisticsCollector

    // =================================================================================================================
    // FLUSHING, COMPACTING & GARBAGE COLLECTION
    // These methods are not exposed in the public API (interface) and primarily serve for automated testing purposes.
    // =================================================================================================================

    fun garbageCollectionSynchronous(monitor: TaskMonitor = TaskMonitor.create()) {
        this.storeManager.performGarbageCollection(monitor)
    }

    fun flushAllStoresAsync(scheduleMinorCompactionOnCompletion: Boolean): CompletableFuture<*> {
        val futures = this.storeManager
            .getAllLsmTreesAdmin()
            .map { it.scheduleMemtableFlush(scheduleMinorCompactionOnCompletion) }
        return CompletableFuture.allOf(*futures.toTypedArray())
    }

    fun flushAllStoresSynchronous() {
        this.flushAllStoresAsync(scheduleMinorCompactionOnCompletion = false).join()
    }

    fun flushAsync(storeId: StoreId, scheduleMinorCompactionOnCompletion: Boolean): CompletableFuture<*> {
        return this.storeManager
            .getStoreByIdAdmin(storeId)
            .scheduleMemtableFlush(scheduleMinorCompactionOnCompletion)
    }

    fun flushAsync(storeId: String, scheduleMinorCompactionOnCompletion: Boolean): CompletableFuture<*> {
        return this.flushAsync(
            storeId = StoreId.of(storeId),
            scheduleMinorCompactionOnCompletion = scheduleMinorCompactionOnCompletion
        )
    }

    fun flushSynchronous(storeId: StoreId) {
        this.flushAsync(storeId, false).join()
    }

    fun flushSynchronous(storeId: String) {
        this.flushSynchronous(StoreId.of(storeId))
    }

    fun majorCompactionOnAllStoresAsync(): CompletableFuture<*> {
        val futures = this.storeManager.getAllLsmTreesAdmin()
            .map { it.scheduleMajorCompaction() }
            .toList()
        return CompletableFuture.allOf(*futures.toTypedArray())
    }

    fun majorCompactionOnAllStoresSynchronous() {
        this.majorCompactionOnAllStoresAsync().join()
    }

    fun majorCompactionOnStoreAsync(storeId: StoreId): CompletableFuture<*> {
        return this.storeManager.getStoreByIdAdmin(storeId).scheduleMajorCompaction()
    }

    fun majorCompactionOnStoreAsync(storeId: String): CompletableFuture<*> {
        return this.majorCompactionOnStoreAsync(StoreId.of(storeId))
    }

    fun majorCompactionOnStoreSynchronous(storeId: StoreId) {
        this.majorCompactionOnStoreAsync(storeId).join()
    }

    fun majorCompactionOnStoreSynchronous(storeId: String) {
        this.majorCompactionOnStoreAsync(storeId).join()
    }

    fun minorCompactionOnAllStoresAsync(): CompletableFuture<*> {
        val futures = this.storeManager.getAllLsmTreesAdmin()
            .map { it.scheduleMinorCompaction() }
            .toList()
        return CompletableFuture.allOf(*futures.toTypedArray())
    }

    fun minorCompactionOnAllStoresSynchronous() {
        this.minorCompactionOnAllStoresAsync().join()
    }

    fun minorCompactionOnStoreAsync(storeId: StoreId): CompletableFuture<*> {
        return this.storeManager.getStoreByIdAdmin(storeId).scheduleMinorCompaction()
    }

    fun minorCompactionOnStoreAsync(storeId: String): CompletableFuture<*> {
        return this.minorCompactionOnStoreAsync(StoreId.of(storeId))
    }

    fun minorCompactionOnStoreSynchronous(storeId: StoreId) {
        this.minorCompactionOnStoreAsync(storeId).join()
    }

    fun minorCompactionOnStoreSynchronous(storeId: String) {
        this.minorCompactionOnStoreAsync(storeId).join()
    }

    private fun warnAboutMinorCompactionDisabled() {
        log.warn {
            "Periodic Minor Compaction is disabled, because the corresponding CRON expression is NULL!" +
                " You need to compact the store explicitly to prevent performance degradation."
        }
    }

    private fun warnAboutMajorCompactionDisabled() {
        log.warn {
            "Periodic Major Compaction is disabled, because the corresponding CRON expression is NULL!" +
                " You need to compact the store explicitly to prevent performance degradation."
        }
    }


}