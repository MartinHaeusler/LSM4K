package org.chronos.chronostore.impl

import mu.KotlinLogging
import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.async.executor.AsyncTaskManagerImpl
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.io.structure.ChronoStoreStructure
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.lsm.cache.BlockCacheManagerImpl
import org.chronos.chronostore.wal.WriteAheadLog
import java.util.concurrent.Executors

class ChronoStoreImpl(
    private val vfs: VirtualFileSystem,
    private val configuration: ChronoStoreConfiguration
) : ChronoStore {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    @Transient
    private var isOpen = true

    private val timeManager = TimeManager()
    private val blockCacheManager = BlockCacheManagerImpl()
    private val storeManager = StoreManagerImpl(
        vfs = this.vfs,
        blockCacheManager = this.blockCacheManager,
        timeManager = this.timeManager,
        blockReadMode = configuration.blockReadMode,
        mergeStrategy = configuration.mergeStrategy,
        driverFactory = configuration.randomFileAccessDriverFactory
    )
    private val taskManager = AsyncTaskManagerImpl(Executors.newFixedThreadPool(configuration.maxWriterThreads))
    private val writeAheadLog: WriteAheadLog
    private val transactionManager: TransactionManager

    init {
        val walFile = this.vfs.file(ChronoStoreStructure.WAL_FILE_NAME)
        this.writeAheadLog = WriteAheadLog(walFile)
        if (!walFile.exists()) {
            // The WAL file doesn't exist. It's a new, empty database.
            // We don't need a recovery, but we have to "set up camp".
            this.createNewEmptyDatabase()
        } else {
            // the WAL file exists, perform startup recovery.
            this.performStartupRecovery()
        }
        this.transactionManager = TransactionManager(
            storeManager = this.storeManager,
            timeManager = this.timeManager,
            writeAheadLog = this.writeAheadLog
        )
    }

    private fun createNewEmptyDatabase() {
        log.info { "Creating a new, empty database in: ${this.vfs}" }
        this.writeAheadLog.createFileIfNotExists()
        this.storeManager.initialize(isEmptyDatabase = true)
    }

    private fun performStartupRecovery() {
        log.info { "Opening database in: ${this.vfs}" }
        // remove any incomplete transactions from the WAL file.
        this.writeAheadLog.performStartupRecoveryCleanup()
        // initialize the store manager so that we can read from it.
        this.storeManager.initialize(isEmptyDatabase = false)
        val allStores = this.storeManager.getAllStoresInternal()
        // with the store info, replay the changes found in the WAL.
        log.info { "Located ${allStores.size} stores belonging to this database." }
        this.replayWriteAheadLogChanges(allStores)
    }

    private fun replayWriteAheadLogChanges(allStores: List<Store>) {
        val storeIdToStore = allStores.associateBy { it.id }
        val storeIdToMaxTimestamp = allStores.associate { store ->
            val maxPersistedTimestamp = (store as StoreImpl).tree.getMaxPersistedTimestamp()
            store.id to maxPersistedTimestamp
        }
        log.info { "Replaying Write Ahead Log file" }
        // replay the entries in the WAL which have not been persisted yet
        var totalTransactions = 0
        var missingTransactions = 0
        this.writeAheadLog.readWal { walTransaction ->
            totalTransactions++
            var changed = false
            for (entry in walTransaction.storeIdToCommands.entries) {
                val storeMaxTimestamp = storeIdToMaxTimestamp[entry.key]
                    ?: continue // the store doesn't exist anymore, skip

                if (storeMaxTimestamp > walTransaction.commitTimestamp) {
                    // we already have these changes in our store.
                    continue
                }

                // we don't have this change yet, apply it
                val store = storeIdToStore[entry.key]
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
    }


    override fun beginTransaction(): ChronoStoreTransaction {
        return this.transactionManager.createNewTransaction()
    }

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

}