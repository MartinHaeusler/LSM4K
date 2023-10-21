package org.chronos.chronostore.impl

import com.google.common.annotations.VisibleForTesting
import mu.KotlinLogging
import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.async.executor.AsyncTaskManagerImpl
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.structure.ChronoStoreStructure
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.lsm.LSMForestMemoryManager
import org.chronos.chronostore.lsm.cache.BlockCacheManager
import org.chronos.chronostore.lsm.cache.FileHeaderCache
import org.chronos.chronostore.lsm.merge.strategy.MergeService
import org.chronos.chronostore.lsm.merge.strategy.MergeServiceImpl
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.wal2.WriteAheadLog2
import java.util.concurrent.Executors
import kotlin.math.max

class ChronoStoreImpl(
    private val vfs: VirtualFileSystem,
    private val configuration: ChronoStoreConfiguration,
) : ChronoStore {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    @Transient
    private var isOpen = true

    private val timeManager: TimeManager
    private val blockCacheManager = BlockCacheManager.create(configuration.blockCacheSize)
    private val fileHeaderCache = FileHeaderCache.create(configuration.fileHeaderCacheSize)
    private val taskManager = AsyncTaskManagerImpl(Executors.newScheduledThreadPool(configuration.maxWriterThreads))

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
        mergeService = this.mergeService,
        forest = this.forest,
        driverFactory = this.configuration.randomFileAccessDriverFactory,
        newFileSettings = ChronoStoreFileSettings(configuration.compressionAlgorithm, configuration.maxBlockSize),
    )

    private val writeAheadLog: WriteAheadLog2
    private val transactionManager: TransactionManager

    init {
        val walDirectory = this.vfs.directory(ChronoStoreStructure.WRITE_AHEAD_LOG_DIR_NAME)
        val isEmptyDatabase = !walDirectory.exists()
        this.writeAheadLog = WriteAheadLog2(walDirectory, this.configuration.compressionAlgorithm, this.configuration.maxWriteAheadLogFileSize.bytes)
        val currentTimestamp = if (isEmptyDatabase) {
            // The WAL file doesn't exist. It's a new, empty database.
            // We don't need a recovery, but we have to "set up camp".
            this.createNewEmptyDatabase()
        } else {
            // the WAL file exists, perform startup recovery.
            this.performStartupRecovery()
        }

        if (currentTimestamp + 1000 > System.currentTimeMillis()) {
            throw IllegalStateException(
                "Last commit timestamp in the database is at ${currentTimestamp} but" +
                    " System clock is at ${System.currentTimeMillis()} and therefore" +
                    " behind the database insertions!"
            )
        }

        this.timeManager = TimeManager(currentTimestamp)

        this.transactionManager = TransactionManager(
            storeManager = this.storeManager,
            timeManager = this.timeManager,
            writeAheadLog = this.writeAheadLog
        )
        this.mergeService.initialize(this.storeManager, writeAheadLog)
    }

    private fun createNewEmptyDatabase(): Timestamp {
        log.info { "Creating a new, empty database in: ${this.vfs}" }
        this.storeManager.initialize(isEmptyDatabase = true)
        return 0L
    }

    private fun performStartupRecovery(): Timestamp {
        log.info { "Opening database in: ${this.vfs}" }
        // initialize the store manager so that we can read from it.
        this.storeManager.initialize(isEmptyDatabase = false)
        // remove any incomplete transactions from the WAL file.
        this.writeAheadLog.performStartupRecoveryCleanup(this.storeManager::getHighWatermarkTimestamp)
        val allStores = this.storeManager.getAllStoresInternal()
        // with the store info, replay the changes found in the WAL.
        log.info { "Located ${allStores.size} stores belonging to this database." }
        return this.replayWriteAheadLogChanges(allStores)
    }

    private fun replayWriteAheadLogChanges(allStores: List<Store>): Timestamp {
        val storeNameToStore = allStores.associateBy { it.name }
        val storeIdToMaxTimestamp = allStores.associate { store ->
            val maxPersistedTimestamp = (store as StoreImpl).tree.getMaxPersistedTimestamp()
            store.name to maxPersistedTimestamp
        }
        log.info { "Replaying Write Ahead Log file" }
        // replay the entries in the WAL which have not been persisted yet
        var totalTransactions = 0
        var missingTransactions = 0
        var maxTimestamp = 0L
        this.writeAheadLog.readWalStreaming { walTransaction ->
            totalTransactions++
            maxTimestamp = max(maxTimestamp, walTransaction.commitTimestamp)
            var changed = false
            for (entry in walTransaction.storeIdToCommands.entries) {
                val storeMaxTimestamp = storeIdToMaxTimestamp[entry.key]
                    ?: continue // the store doesn't exist anymore, skip

                if (storeMaxTimestamp > walTransaction.commitTimestamp) {
                    // we already have these changes in our store.
                    continue
                }

                // we don't have this change yet, apply it
                val store = storeNameToStore[entry.key]
                    ?: continue // the store doesn't exist anymore, skip

                // we're missing the changes from this transaction,
                // put them into the store.
                (store as StoreImpl).tree.put(entry.value)
                changed = true
            }
            if (changed) {
                missingTransactions++
            }
        }
        log.info { "Successfully replayed ${totalTransactions} transactions in the WAL file. ${missingTransactions} transactions were missing in persistent files." }
        return maxTimestamp
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
        this.transactionManager.close()
        this.taskManager.close()
        this.storeManager.close()
        log.info { "Completed shut-down of ChronoStore at '${this.vfs}'." }
    }

    fun performGarbageCollection(monitor: TaskMonitor = TaskMonitor.create()) {
        this.storeManager.performGarbageCollection(monitor)
    }

}