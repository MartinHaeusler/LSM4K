package org.chronos.chronostore.impl

import mu.KotlinLogging
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.api.SystemStore
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.forEachWithMonitor
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.structure.ChronoStoreStructure.STORE_INFO_FILE_NAME
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.lsm.LSMForestMemoryManager
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.lsm.cache.BlockCacheManager
import org.chronos.chronostore.lsm.cache.FileHeaderCache
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.json.JsonUtil
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.math.min

class StoreManagerImpl(
    private val vfs: VirtualFileSystem,
    private val blockCacheManager: BlockCacheManager,
    private val fileHeaderCache: FileHeaderCache,
    private val forest: LSMForestMemoryManager,
    private val driverFactory: RandomFileAccessDriverFactory,
    private val newFileSettings: ChronoStoreFileSettings,
) : StoreManager, AutoCloseable {

    companion object {

        private const val DB_ALREADY_CLOSED = "This database has already been closed."

        private val log = KotlinLogging.logger {}

    }

    @Transient
    private var isOpen = true

    private var initialized: Boolean = false
    private val storeInfoFile = vfs.file(STORE_INFO_FILE_NAME)
    private val storesById = mutableMapOf<StoreId, Store>()
    private val lock = ReentrantReadWriteLock(true)

    fun initialize(isEmptyDatabase: Boolean) {
        this.lock.write {
            check(!this.initialized) { "This StoreManager has already been initialized!" }
            if (isEmptyDatabase) {
                check(!this.storeInfoFile.exists()) {
                    "An empty database is being created, but the store info file exists: ${storeInfoFile.path}"
                }
                log.info { "Creating a new, empty database in '${this.vfs.rootPath}'." }
                this.storeInfoFile.create()
                // in the beginning, there are no stores.
                this.overwriteStoreInfoJson(emptyList())
            } else {
                log.info { "Opening an existing database in '${this.vfs.rootPath}'." }
                // read the store infos from the JSON file
                val storeInfos = this.readStoreInfoFromJson()
                for (storeInfo in storeInfos) {
                    val storeId = StoreId.of(storeInfo.storeId)
                    val storeDirectory = getStoreDirectory(storeId)
                    val store = createStore(storeInfo, storeDirectory)
                    this.registerStoreInCaches(store)
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
        val names = storeId.path
        return if (names.size == 1) {
            this.vfs.directory(names[0])
        } else {
            val rootName = names[0]
            val rootDir = this.vfs.directory(rootName)
            names.asSequence().drop(1).fold(rootDir) { currentDir, name ->
                currentDir.directory(name)
            }
        }
    }

    private fun createStore(storeInfo: StoreInfo, directory: VirtualDirectory): StoreImpl {
        val storeId = StoreId.of(storeInfo.storeId)
        return StoreImpl(
            storeId = storeId,
            retainOldVersions = storeInfo.retainOldVersions,
            validFrom = storeInfo.validFrom,
            validTo = storeInfo.validTo,
            createdByTransactionId = storeInfo.createdByTransactionId,
            directory = directory,
            forest = this.forest,
            blockCache = this.blockCacheManager.getBlockCache(storeId),
            fileHeaderCache = this.fileHeaderCache,
            driverFactory = this.driverFactory,
            newFileSettings = this.newFileSettings,
        )
    }

    override fun <T> withStoreReadLock(action: () -> T): T {
        return this.lock.read(action)
    }

    override fun getStoreByNameOrNull(transaction: ChronoStoreTransaction, name: StoreId): Store? {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.lock.read {
            assertInitialized()
            return this.storesById[name]?.takeIf { it.isVisibleFor(transaction) }
        }
    }

    override fun createNewStore(transaction: ChronoStoreTransaction, name: StoreId, versioned: Boolean, validFrom: Timestamp): Store {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        check(transaction.isOpen) { "Argument 'transaction' must refer to an open transaction, but the given transaction has already been closed!" }
        require(validFrom >= transaction.lastVisibleTimestamp) { "Argument 'validFrom' (${validFrom}) must not be smaller than the transaction timestamp (${transaction.lastVisibleTimestamp})!" }
        this.lock.write {
            assertInitialized()
            require(!name.isSystemInternal) { "Store names must not start with '_' (reserved for internal purposes)!" }
            require(!this.storesById.containsKey(name)) { "There already exists a store with StoreID '${name}'!" }
            // ensure that stores are not becoming nested.
            val allStores = this.getAllStoresInternal()
            val offendingStore = allStores.firstOrNull { name.collidesWith(it.storeId) }
            require(offendingStore == null) {
                "The given StoreID '${name}' collides with existing StoreID '${offendingStore}' - StoreID paths must not be prefixes of one another (Stores cannot be 'nested')!"
            }
            val transactionId = transaction.id
            return createAndRegisterStoreInternal(name, versioned, validFrom, transactionId)
        }
    }

    private fun createAndRegisterStoreInternal(
        storeId: StoreId,
        versioned: Boolean,
        validFrom: Timestamp,
        transactionId: TransactionId
    ): StoreImpl {
        // do a small probing by creating the directory, but don't do anything with it just yet.
        // We still have to update the storeInfo.json first before going ahead. Creating the
        // directory first helps us sort out file system issues (path too long, folder permission issues, etc.)
        val directory = this.getStoreDirectory(storeId)
        directory.mkdirs()

        val storeInfoList = mutableListOf<StoreInfo>()
        this.storesById.values.asSequence().map { it.storeInfo }.toCollection(storeInfoList)
        val newStoreInfo = StoreInfo(
            storeId = storeId.toString(),
            retainOldVersions = versioned,
            validFrom = validFrom,
            validTo = null,
            createdByTransactionId = transactionId
        )
        storeInfoList.add(newStoreInfo)

        // first, write the store info into our JSON file. This will ensure that
        // the store still exists upon the next database restart.
        this.overwriteStoreInfoJson(storeInfoList)

        // then, create the store itself
        val store = this.createStore(newStoreInfo, directory)

        this.registerStoreInCaches(store)
        return store
    }

    override fun getSystemStore(systemStore: SystemStore): Store {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.lock.read {
            assertInitialized()

            return this.storesById[systemStore.storeId]
                ?: throw IllegalStateException("System Store '${systemStore}' was not initialized!")
        }
    }

    private fun ensureSystemStoreExists(systemStore: SystemStore): Store {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.lock.write {
            val existingStore = this.storesById[systemStore.storeId]
            if (existingStore != null) {
                return existingStore
            }
            return this.createAndRegisterStoreInternal(
                storeId = systemStore.storeId,
                versioned = systemStore.isVersioned,
                validFrom = 0L,
                transactionId = TransactionId.fromString("11111111-1111-1111-2222-000000000001")
            )
        }
    }

    override fun deleteStore(transaction: ChronoStoreTransaction, name: StoreId): Boolean {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.lock.write {
            assertInitialized()
            val store = this.getStoreByNameOrNull(transaction, name)
                ?: return false
            val storeInfoList = this.storesById.values.asSequence()
                .filter { it.storeId != store.storeId }
                .map { it.storeInfo }
                .toList()

            this.overwriteStoreInfoJson(storeInfoList)
            this.storesById.remove(store.storeId)
            store.directory.clear()
            store.directory.delete()
            return true
        }
    }

    /**
     * Returns all stores.
     *
     * Internal method which returns all stores, regardless of the [Store.validFrom] timestamp.
     *
     * Public API users should use [getAllStores] instead.
     *
     * @return A list of all stores. The list is a snapshot copy of the current state and doesn't change when stores are added/removed.
     */
    fun getAllStoresInternal(): List<Store> {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.lock.read {
            this.assertInitialized()
            return this.storesById.values.toList()
        }
    }

    override fun getAllLsmTrees(): List<LSMTree> {
        return this.getAllStoresInternal().map { (it as StoreImpl).tree }
    }

    override fun getAllStores(transaction: ChronoStoreTransaction): List<Store> {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        return this.getAllStoresInternal().filter { it.isVisibleFor(transaction) }
    }

    override fun performGarbageCollection(monitor: TaskMonitor) {
        monitor.reportStarted("Performing LSM Garbage Collection")
        if (!this.isOpen) {
            monitor.reportDone()
            return
        }
        this.lock.read {
            monitor.forEachWithMonitor(1.0, "Cleaning up old files", this.storesById.values.toList()) { taskMonitor, store ->
                store as StoreImpl
                store.tree.performGarbageCollection(taskMonitor)
            }
        }
        monitor.reportDone()
    }

    override fun getHighWatermarkTimestamp(): Timestamp {
        this.lock.read {
            val latestTimestamp = this.storesById.values.asSequence()
                .mapNotNull { it.highWatermarkTimestamp }
                .maxOrNull()
            return latestTimestamp ?: -1L
        }
    }

    override fun getLowWatermarkTimestamp(): Timestamp {
        this.lock.read {
            val highWatermark = this.getHighWatermarkTimestamp()
            val minLowWatermark = this.storesById.values.asSequence()
                .filter(Store::hasInMemoryChanges)
                .mapNotNull { it.lowWatermarkTimestamp }
                .minOrNull()

            if (minLowWatermark == null) {
                // no store has in-memory changes
                // => everything is 100% persisted
                // => low watermark == high watermark
                return highWatermark
            } else {
                // some stores have in-memory changes.
                // As a precaution, limit the low watermark with the
                // high watermark.
                return min(highWatermark, minLowWatermark)
            }
        }
    }

    override fun close() {
        if (!this.isOpen) {
            return
        }
        this.isOpen = false
    }

    private fun overwriteStoreInfoJson(storeInfos: List<StoreInfo>) {
        this.lock.write {
            this.storeInfoFile.createOverWriter().use { overWriter ->
                JsonUtil.writeJson(storeInfos, overWriter.outputStream)
                overWriter.commit()
            }
        }
    }

    private fun readStoreInfoFromJson(): List<StoreInfo> {
        this.lock.read {
            this.storeInfoFile.withInputStream { input ->
                return JsonUtil.readJsonAsObject<List<StoreInfo>>(input)
            }
        }
    }

    private fun registerStoreInCaches(store: Store) {
        this.lock.write {
            this.storesById[store.storeId] = store
        }
    }

    private fun assertInitialized() {
        check(this.initialized) { "The StoreManager has not yet been initialized!" }
    }

    private val Store.storeInfo: StoreInfo
        get() = StoreInfo(
            storeId = storeId.toString(),
            retainOldVersions = retainOldVersions,
            validFrom = validFrom,
            validTo = validTo,
            createdByTransactionId = createdByTransactionId
        )

    private fun Store.isVisibleFor(transaction: ChronoStoreTransaction): Boolean {
        val validFrom = this.validFrom
        val validTo = this.validTo ?: Timestamp.MAX_VALUE
        val txTimestamp = transaction.lastVisibleTimestamp
        val createdByThisTransaction = this.createdByTransactionId == transaction.id
        return (validFrom < txTimestamp || createdByThisTransaction) && (txTimestamp < validTo || this.retainOldVersions)
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

    private data class StoreInfo(
        val storeId: String,
        val retainOldVersions: Boolean,
        val validFrom: Timestamp,
        val validTo: Timestamp?,
        val createdByTransactionId: TransactionId
    ) {

        val terminated: Boolean
            get() = validTo != null

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as StoreInfo

            return storeId == other.storeId
        }

        override fun hashCode(): Int {
            return storeId.hashCode()
        }
    }

}