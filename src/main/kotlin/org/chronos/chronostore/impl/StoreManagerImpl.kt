package org.chronos.chronostore.impl

import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronostore.api.*
import org.chronos.chronostore.api.compaction.CompactionStrategy
import org.chronos.chronostore.api.exceptions.StoreNotFoundException
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.forEachWithMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.mainTask
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.lsm.LSMForestMemoryManager
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.lsm.cache.BlockCacheManager
import org.chronos.chronostore.lsm.cache.FileHeaderCache
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.manifest.operations.DeleteStoreOperation
import org.chronos.chronostore.util.ManagerState
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.TransactionId
import org.pcollections.TreePMap
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class StoreManagerImpl(
    private val vfs: VirtualFileSystem,
    private val blockCacheManager: BlockCacheManager,
    private val fileHeaderCache: FileHeaderCache,
    private val forest: LSMForestMemoryManager,
    private val driverFactory: RandomFileAccessDriverFactory,
    private val newFileSettings: ChronoStoreFileSettings,
    private val configuration: ChronoStoreConfiguration,
    private val manifestFile: ManifestFile,
    private val getSmallestOpenReadTSN: () -> TSN?,
    private val killswitch: Killswitch,
) : StoreManager, AutoCloseable {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    @Transient
    private var state: ManagerState = ManagerState.OPEN

    private var initialized: Boolean = false
    private val storesById = mutableMapOf<StoreId, Store>()
    private val storeManagementLock = ReentrantReadWriteLock(true)

    private val compactionWorkerPool = Executors.newScheduledThreadPool(this.configuration.maxCompactionThreads)
    private val memtableFlushWorkerPool = Executors.newScheduledThreadPool(this.configuration.maxMemtableFlushThreads)

    fun initialize(isEmptyDatabase: Boolean) {
        this.storeManagementLock.write {
            check(!this.initialized) { "This StoreManager has already been initialized!" }
            if (isEmptyDatabase) {
                log.info { "Creating a new, empty database in '${this.vfs.rootPath}'." }
                // in the beginning, there are no stores.
            } else {
                log.info { "Opening an existing database in '${this.vfs.rootPath}'." }
                // read the store infos from the JSON file
                val manifest = this.manifestFile.getManifest()
                for (storeMetadata in manifest.stores.values) {
                    val storeId = storeMetadata.storeId
                    val storeDirectory = this.getStoreDirectory(storeId)
                    val store = this.createStore(storeMetadata, storeDirectory)
                    this.registerStore(store)
                }
            }
            // ensure that the system stores exist
            for (systemStore in SystemStore.entries) {
                this.ensureSystemStoreExists(systemStore)
            }

            log.info { "Store initialization performed successfully for ${this.storesById.size} stores." }
            this.initialized = true
        }
    }

    private fun getStoreDirectory(storeId: StoreId): VirtualDirectory {
        return this.vfs.directory(storeId.path)
    }

    private fun createStore(storeMetadata: StoreMetadata, directory: VirtualDirectory): StoreImpl {
        return StoreImpl(
            initialStoreMetadata = storeMetadata,
            manifestFile = this.manifestFile,
            directory = directory,
            compactionExecutor = this.compactionWorkerPool,
            memtableFlushExecutor = this.memtableFlushWorkerPool,
            forest = this.forest,
            blockCache = this.blockCacheManager.getBlockCache(storeMetadata.storeId),
            fileHeaderCache = this.fileHeaderCache,
            driverFactory = this.driverFactory,
            newFileSettings = this.newFileSettings,
            getSmallestOpenReadTSN = this.getSmallestOpenReadTSN,
            killswitch = this.killswitch,
        )
    }

    override fun <T> withStoreReadLock(action: () -> T): T {
        return this.storeManagementLock.read(action)
    }

    fun getStoreByIdAdmin(storeId: String): StoreImpl {
        return this.getStoreByIdAdmin(StoreId.of(storeId))
    }

    fun getStoreByIdAdmin(storeId: StoreId): StoreImpl {
        return this.getStoreByIdOrNullAdmin(storeId)
            ?: throw StoreNotFoundException("There is no store with ID '${storeId}'!")
    }

    fun getStoreByIdOrNullAdmin(storeId: String): StoreImpl? {
        return this.getStoreByIdOrNullAdmin(StoreId.of(storeId))
    }

    fun getStoreByIdOrNullAdmin(storeId: StoreId): StoreImpl? {
        this.state.checkOpen()
        this.storeManagementLock.read {
            val store = this.storesById[storeId]
                ?: return null
            return store as StoreImpl
        }
    }

    override fun getStoreByIdOrNull(transaction: ChronoStoreTransaction, name: StoreId): Store? {
        this.state.checkOpen()
        this.storeManagementLock.read {
            assertInitialized()
            return this.storesById[name]?.takeIf { it.isVisibleFor(transaction) }
        }
    }

    override fun createNewStore(
        transaction: ChronoStoreTransaction,
        storeId: StoreId,
        validFromTSN: TSN,
        compactionStrategy: CompactionStrategy?,
    ): Store {
        this.state.checkOpen()
        check(transaction.isOpen) { "Argument 'transaction' must refer to an open transaction, but the given transaction has already been closed!" }
        require(validFromTSN >= transaction.lastVisibleSerialNumber) { "Argument 'validFrom' (${validFromTSN}) must not be smaller than the transaction last visible TSN (${transaction.lastVisibleSerialNumber})!" }
        this.storeManagementLock.write {
            assertInitialized()
            require(!storeId.isSystemInternal) { "Store names must not start with '_' (reserved for internal purposes)!" }
            require(!this.storesById.containsKey(storeId)) { "There already exists a store with StoreID '${storeId}'!" }
            // ensure that stores are not becoming nested.
            val allStores = this.getAllStoresInternal()
            val offendingStore = allStores.firstOrNull { storeId.collidesWith(it.storeId) }
            require(offendingStore == null) {
                "The given StoreID '${storeId}' collides with existing StoreID '${offendingStore}' - StoreID paths must not be prefixes of one another (Stores cannot be 'nested')!"
            }
            val transactionId = transaction.id
            return createAndRegisterStoreInternal(
                storeId = storeId,
                validFromTSN = validFromTSN,
                transactionId = transactionId,
                compactionStrategy = compactionStrategy
            )
        }
    }

    private fun createAndRegisterStoreInternal(
        storeId: StoreId,
        validFromTSN: TSN,
        transactionId: TransactionId,
        compactionStrategy: CompactionStrategy?,
    ): StoreImpl {
        // do a small probing by creating the directory, but don't do anything with it just yet.
        // We still have to update the storeInfo.json first before going ahead. Creating the
        // directory first helps us sort out file system issues (path too long, folder permission issues, etc.)
        val directory = this.getStoreDirectory(storeId)
        directory.mkdirs()

        val newStoreInfo = StoreInfo(
            storeId = storeId,
            validFromTSN = validFromTSN,
            validToTSN = null,
            createdByTransactionId = transactionId
        )

        // first, write the store info into our JSON file. This will ensure that
        // the store still exists upon the next database restart.
        val newStoreMetadata = StoreMetadata(
            info = newStoreInfo,
            lsmFiles = TreePMap.empty(),
            compactionStrategy = compactionStrategy ?: this.configuration.defaultCompactionStrategy,
        )
        this.manifestFile.appendCreateStoreOperation(newStoreMetadata)

        // then, create the store object itself
        val store = createStore(newStoreMetadata, directory)
        this.registerStore(store)
        return store
    }

    override fun getSystemStore(systemStore: SystemStore): Store {
        this.state.checkOpen()
        this.storeManagementLock.read {
            assertInitialized()
            this.manifestFile.getManifest().getStoreOrNull(systemStore.storeId)
            return this.storesById[systemStore.storeId]
                ?: throw IllegalStateException("System Store '${systemStore}' was not initialized!")
        }
    }

    private fun ensureSystemStoreExists(systemStore: SystemStore): Store {
        this.state.checkOpen()
        this.storeManagementLock.write {
            val existingStore = this.storesById[systemStore.storeId]
            if (existingStore != null) {
                return existingStore
            }
            return this.createAndRegisterStoreInternal(
                storeId = systemStore.storeId,
                validFromTSN = 0L,
                transactionId = TransactionId.fromString("11111111-1111-1111-2222-000000000001"),
                compactionStrategy = null, // use default
            )
        }
    }

    override fun deleteStore(transaction: ChronoStoreTransaction, storeId: StoreId): Boolean {
        this.state.checkOpen()
        this.storeManagementLock.write {
            assertInitialized()
            val store = this.getStoreByIdOrNull(transaction, storeId)
                ?: return false

            this.manifestFile.appendOperation { sequenceNumber ->
                DeleteStoreOperation(
                    sequenceNumber = sequenceNumber,
                    storeId = storeId,
                    terminatingTSN = transaction.lastVisibleSerialNumber,
                )
            }

            this.storesById.remove(store.storeId)
            store.directory.clear()
            store.directory.delete()
            return true
        }
    }

    /**
     * Returns all stores.
     *
     * Internal method which returns all stores, regardless of the [Store.validFromTSN].
     *
     * Public API users should use [getAllStores] instead.
     *
     * @return A list of all stores. The list is a snapshot copy of the current state and doesn't change when stores are added/removed.
     */
    fun getAllStoresInternal(): List<Store> {
        this.state.checkOpen()
        this.storeManagementLock.read {
            this.assertInitialized()
            return this.storesById.values.toList()
        }
    }

    fun getAllLsmTreesAdmin(): List<LSMTree> {
        this.state.checkOpen()
        return this.getAllStoresInternal().map { (it as StoreImpl).tree }
    }

    override fun getAllStores(transaction: ChronoStoreTransaction): List<Store> {
        this.state.checkOpen()
        return this.getAllStoresInternal().filter { it.isVisibleFor(transaction) }
    }

    override fun performGarbageCollection(monitor: TaskMonitor) = monitor.mainTask("Perform LSM Garbage Collection") {
        if (this.state.isClosed()) {
            return
        }
        this.storeManagementLock.read {
            monitor.forEachWithMonitor(1.0, "Cleaning up old files", this.storesById.values.toList()) { taskMonitor, store ->
                store as StoreImpl
                store.tree.performGarbageCollection(taskMonitor)
            }
        }
    }

    override fun getHighWatermarkTSN(): TSN? {
        this.storeManagementLock.read {
            return storesById.values.asSequence()
                .mapNotNull { it.highWatermarkTSN }
                .maxOrNull()
        }
    }

    override fun getLowWatermarkTSN(): TSN? {
        this.storeManagementLock.read {
            return this.storesById.values.asSequence()
                .mapNotNull { it.lowWatermarkTSN }
                .minOrNull()
        }
    }

    override fun close() {
        this.closeInternal(ManagerState.CLOSED)
    }

    fun closePanic() {
        this.closeInternal(ManagerState.PANIC)
    }

    private fun closeInternal(closeState: ManagerState) {
        if (this.state.isClosed()) {
            this.state = closeState
            return
        }
        this.state = closeState
        try {
            this.compactionWorkerPool.shutdownNow()
        } catch (e: Exception) {
            log.warn { "An exception occurred during ChronoStore shutdown: ${e}" }
        }
        try {
            this.memtableFlushWorkerPool.shutdownNow()
        } catch (e: Exception) {
            log.warn { "An exception occurred during ChronoStore shutdown: ${e}" }
        }
    }

    private fun registerStore(store: Store) {
        this.storeManagementLock.write {
            this.storesById[store.storeId] = store
        }
    }

    private fun assertInitialized() {
        check(this.initialized) { "The StoreManager has not yet been initialized!" }
    }

    private fun Store.isVisibleFor(transaction: ChronoStoreTransaction): Boolean {
        val validFrom = this.validFromTSN
        val validTo = this.validToTSN ?: TSN.MAX_VALUE
        val txTSN = transaction.lastVisibleSerialNumber
        val createdByThisTransaction = this.createdByTransactionId == transaction.id
        return (validFrom < txTSN || createdByThisTransaction) && (txTSN < validTo)
    }

    private fun StoreId.collidesWith(other: StoreId): Boolean {
        if (this == other) {
            return true
        }
        val thisIterator = this.path.iterator()
        val otherIterator = other.path.iterator()
        while (thisIterator.hasNext() && otherIterator.hasNext()) {
            val thisElement = thisIterator.next()
            val otherElement = otherIterator.next()
            if (thisElement != otherElement) {
                return false
            }
        }
        // one of them is a prefix of the other!
        return true
    }

}