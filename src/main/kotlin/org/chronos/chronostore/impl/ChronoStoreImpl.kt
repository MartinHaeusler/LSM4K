package org.chronos.chronostore.impl

import com.google.common.annotations.VisibleForTesting
import mu.KotlinLogging
import org.chronos.chronostore.api.*
import org.chronos.chronostore.async.executor.AsyncTaskManagerImpl
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.checkpoint.CheckpointManager
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.structure.ChronoStoreStructure
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.lsm.LSMForestMemoryManager
import org.chronos.chronostore.lsm.cache.BlockCacheManager
import org.chronos.chronostore.lsm.cache.FileHeaderCache
import org.chronos.chronostore.lsm.garbagecollector.tasks.GarbageCollectorTask
import org.chronos.chronostore.lsm.merge.strategy.MergeService
import org.chronos.chronostore.lsm.merge.strategy.MergeServiceImpl
import org.chronos.chronostore.lsm.merge.tasks.CheckpointTask
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.wal.WriteAheadLog
import java.util.concurrent.Executors
import kotlin.math.max

class ChronoStoreImpl(
    private val vfs: VirtualFileSystem,
    private val configuration: ChronoStoreConfiguration,
) : ChronoStore {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    @Volatile
    private var isOpen = true

    @Volatile
    private var state: ChronoStoreState = ChronoStoreState.STARTING

    private val checkpointManager: CheckpointManager
    private val tsnManager: TSNManager
    private val blockCacheManager = BlockCacheManager.create(configuration.blockCacheSize)
    private val fileHeaderCache = FileHeaderCache.create(configuration.fileHeaderCacheSize)

    private val taskManager = AsyncTaskManagerImpl(
        executorService = Executors.newScheduledThreadPool(this.configuration.maxWriterThreads),
        getChronoStoreState = this::state,
    )

    @VisibleForTesting
    val mergeService: MergeService = MergeServiceImpl(this.taskManager, this.configuration)

    val forest: LSMForestMemoryManager = LSMForestMemoryManager(
        asyncTaskManager = this.taskManager,
        maxForestSize = this.configuration.maxForestSize.bytes,
        flushThresholdSize = (this.configuration.maxForestSize.bytes * this.configuration.forestFlushThreshold).toLong()
    )

    private val storeManager = StoreManagerImpl(
        vfs = this.vfs,
        blockCacheManager = this.blockCacheManager,
        fileHeaderCache = this.fileHeaderCache,
        forest = this.forest,
        driverFactory = this.configuration.randomFileAccessDriverFactory,
        newFileSettings = ChronoStoreFileSettings(configuration.compressionAlgorithm, configuration.maxBlockSize),
    )

    private val writeAheadLog: WriteAheadLog
    private val transactionManager: TransactionManager


    private val checkpointTask: CheckpointTask

    init {
        val systemDir = this.vfs.directory(SystemStore.PATH_PREFIX)
        val walDirectory = systemDir.directory(ChronoStoreStructure.WRITE_AHEAD_LOG_DIR_NAME)
        val isEmptyDatabase = !walDirectory.exists()
        if (isEmptyDatabase) {
            // If there is no WAL directory, we're going to set up a completely new database.
            // To avoid any trouble with existing files, we demand that our working directory
            // must be empty.
            if (this.vfs.listRootLevelElements().isNotEmpty()) {
                throw IllegalArgumentException("Cannot create new database in '${vfs.rootPath}': the directory is not empty!")
            }
        }
        systemDir.mkdirs()
        this.checkpointManager = CheckpointManager(
            directory = systemDir.directory(ChronoStoreStructure.CHECKPOINT_DIR_NAME),
            maxCheckpointFiles = this.configuration.maxCheckpointFiles,
        )
        this.writeAheadLog = WriteAheadLog(
            directory = walDirectory,
            compressionAlgorithm = this.configuration.compressionAlgorithm,
            maxWalFileSizeBytes = this.configuration.maxWriteAheadLogFileSize.bytes,
            minNumberOfFiles = this.configuration.minNumberOfWriteAheadLogFiles
        )
        val currentTSN = if (isEmptyDatabase) {
            // The WAL file doesn't exist. It's a new, empty database.
            // We don't need a recovery, but we have to "set up camp".
            this.createNewEmptyDatabase()
        } else {
            // the WAL file exists, perform startup recovery.
            this.performStartupRecovery()
        }

        // let the store manager know that watermark queries are valid from now on.
        // Watermark queries deliver invalid results when performed before the WAL
        // recovery has been performed.
        this.storeManager.enableWatermarks()

        this.tsnManager = TSNManager(currentTSN)

        this.transactionManager = TransactionManager(
            storeManager = this.storeManager,
            tsnManager = this.tsnManager,
            writeAheadLog = this.writeAheadLog
        )

        this.checkpointTask = CheckpointTask(
            writeAheadLog = this.writeAheadLog,
            checkpointManager = this.checkpointManager,
            storeManager = this.storeManager
        )
        val checkpointCron = this.configuration.checkpointCron
        if (checkpointCron != null) {
            this.taskManager.scheduleRecurringWithCron(checkpointTask, checkpointCron)
        }

        val garbageCollectorTask = GarbageCollectorTask(storeManager)
        val garbageCollectionCron = this.configuration.garbageCollectionCron
        if (garbageCollectionCron != null) {
            this.taskManager.scheduleRecurringWithCron(garbageCollectorTask, garbageCollectionCron)
        }

        this.mergeService.initialize(this.storeManager, writeAheadLog)
        this.state = ChronoStoreState.RUNNING
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
        val checkpoint = this.checkpointManager.getLatestCheckpoint()
        if (checkpoint != null) {
            // remove superfluous WAL files with the data from the checkpoint
            this.writeAheadLog.shorten(checkpoint.lowWatermark)
        }

        // remove any incomplete transactions from the WAL file.
        this.writeAheadLog.performStartupRecoveryCleanup(this.storeManager::getHighWatermarkTSN)
        val allStores = this.storeManager.getAllStoresInternal()
        // with the store info, replay the changes found in the WAL.
        return this.replayWriteAheadLogChanges(allStores)
    }

    private fun replayWriteAheadLogChanges(allStores: List<Store>): TSN {
        val storeNameToStore = allStores.associateBy { it.storeId }
        val storeIdToMaxTSN = allStores.associate { store ->
            val maxPersistedTSN = (store as StoreImpl).tree.getMaxPersistedTSN()
            store.storeId to maxPersistedTSN
        }
        log.info { "Replaying Write-Ahead-Log" }
        // replay the entries in the WAL which have not been persisted yet
        var totalTransactions = 0
        var missingTransactions = 0
        var maxTSN = 0L
        val timeBefore = System.currentTimeMillis()
        this.writeAheadLog.readWalStreaming { walTransaction ->
            totalTransactions++
            maxTSN = max(maxTSN, walTransaction.commitTSN)
            var changed = false
            for (entry in walTransaction.storeIdToCommands.entries) {
                val storeMaxTimestamp = storeIdToMaxTSN[entry.key]
                    ?: continue // the store doesn't exist anymore, skip

                if (storeMaxTimestamp >= walTransaction.commitTSN) {
                    // we already have these changes in our store.
                    continue
                }

                // we don't have this change yet, apply it
                val store = storeNameToStore[entry.key]
                    ?: continue // the store doesn't exist anymore, skip

                // we're missing the changes from this transaction,
                // put them into the store. Note that we put ALL changes
                // by this transaction into the store at once. This is
                // important because it avoids flushing partial transactions
                // to the LSM tree files.
                (store as StoreImpl).tree.putAll(entry.value)
                changed = true
            }
            if (changed) {
                missingTransactions++
            }
        }
        val timeAfter = System.currentTimeMillis()
        log.info { "Successfully replayed ${totalTransactions} transactions in the WAL file in ${timeAfter - timeBefore}ms. ${missingTransactions} transactions were missing in store files." }
        return maxTSN
    }


    override fun beginTransaction(): ChronoStoreTransaction {
        return this.transactionManager.createNewTransaction()
    }

    override val rootPath: String
        get() = this.vfs.rootPath

    override fun close() {
        if (!isOpen) {
            return
        }
        isOpen = false
        log.info { "Initiating shut-down of ChronoStore at '${this.vfs}'." }
        this.state = ChronoStoreState.SHUTTING_DOWN
        // to ensure a quick startup recovery, ensure that we have a recent checkpoint.
        // If this operation is aborted by process kill, nothing bad happens except
        // that startup recovery may take a bit longer.
        if (this.configuration.checkpointOnShutdown) {
            this.checkpointTask.run(TaskMonitor.create())
        }
        this.transactionManager.close()
        this.taskManager.close()
        this.storeManager.close()
        log.info { "Completed shut-down of ChronoStore at '${this.vfs}'." }
    }

    fun performGarbageCollection(monitor: TaskMonitor = TaskMonitor.create()) {
        this.storeManager.performGarbageCollection(monitor)
    }

}