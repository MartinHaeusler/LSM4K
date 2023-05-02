package org.chronos.chronostore.impl

import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.forEachWithMonitor
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.format.datablock.BlockReadMode
import org.chronos.chronostore.io.structure.ChronoStoreStructure.STORE_DIR_PREFIX
import org.chronos.chronostore.io.structure.ChronoStoreStructure.STORE_INFO_FILE_NAME
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.lsm.cache.BlockCacheManager
import org.chronos.chronostore.lsm.merge.strategy.MergeService
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.json.JsonUtil
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class StoreManagerImpl(
    private val vfs: VirtualFileSystem,
    private val blockCacheManager: BlockCacheManager,
    private val timeManager: TimeManager,
    private val blockReadMode: BlockReadMode,
    private val mergeService: MergeService,
    private val driverFactory: RandomFileAccessDriverFactory,
    private val newFileSettings: ChronoStoreFileSettings,
) : StoreManager, AutoCloseable {

    companion object {

        private const val DB_ALREADY_CLOSED = "This database has already been closed."

    }

    @Transient
    private var isOpen = true

    private var initialized: Boolean = false
    private val storeInfoFile = vfs.file(STORE_INFO_FILE_NAME)
    private val storesById = mutableMapOf<StoreId, Store>()
    private val storesByName = mutableMapOf<String, Store>()
    private val lock = ReentrantReadWriteLock(true)


    fun initialize(isEmptyDatabase: Boolean) {
        this.lock.write {
            if (isEmptyDatabase) {
                check(!this.storeInfoFile.exists()) {
                    "An empty database is being created, but the store info file exists: ${storeInfoFile.path}"
                }
                this.storeInfoFile.create()
                // in the beginning, there are no stores.
                this.overwriteStoreInfoJson(emptyList())
            } else {
                // read the store infos from the JSON file
                val storeInfos = this.readStoreInfoFromJson()
                for (storeInfo in storeInfos) {
                    val store = StoreImpl(
                        id = storeInfo.storeId,
                        name = storeInfo.storeName,
                        retainOldVersions = storeInfo.retainOldVersions,
                        validFrom = storeInfo.validFrom,
                        validTo = storeInfo.validTo,
                        createdByTransactionId = storeInfo.createdByTransactionId,
                        directory = vfs.directory(STORE_DIR_PREFIX + storeInfo.storeId),
                        blockReadMode = this.blockReadMode,
                        mergeService = this.mergeService,
                        blockCache = this.blockCacheManager.getBlockCache(storeInfo.storeId),
                        driverFactory = this.driverFactory,
                        newFileSettings = this.newFileSettings,
                    )
                    this.registerStoreInCaches(store)
                }
            }
            this.initialized = true
        }
    }

    override fun <T> withStoreReadLock(action: () -> T): T {
        return this.lock.read(action)
    }

    override fun getStoreByNameOrNull(transaction: ChronoStoreTransaction, name: String): Store? {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.lock.read {
            assertInitialized()
            return this.storesByName[name]?.takeIf { it.isVisibleFor(transaction) }
        }
    }

    override fun getStoreByIdOrNull(transaction: ChronoStoreTransaction, storeId: StoreId): Store? {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.lock.read {
            assertInitialized()
            return this.storesById[storeId]?.takeIf { it.isVisibleFor(transaction) }
        }
    }

    override fun createNewStore(transaction: ChronoStoreTransaction, name: String, versioned: Boolean): Store {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.lock.write {
            assertInitialized()
            if (this.storesByName.containsKey(name)) {
                throw IllegalArgumentException("There already exists a store with name '${name}'!")
            }
            val storeId = UUID.randomUUID()
            // do a small probing by creating the directory, but don't do anything with it just yet.
            // We still have to update the storeInfo.json first before going ahead. Creating the
            // directory first helps us sort out file system issues (invalid names, path too long, folder permission issues, etc.)
            val directory = vfs.directory(STORE_DIR_PREFIX + storeId)
            directory.mkdirs()

            val storeInfoList = mutableListOf<StoreInfo>()
            this.storesById.values.asSequence().map { it.storeInfo }.toCollection(storeInfoList)
            val newStoreInfo = StoreInfo(
                storeId = storeId,
                storeName = name,
                retainOldVersions = versioned,
                validFrom = this.timeManager.getUniqueWallClockTimestamp(),
                validTo = null,
                createdByTransactionId = transaction.id
            )
            storeInfoList.add(newStoreInfo)

            // first, write the store info into our JSON file. This will ensure that
            // the store still exists upon the next database restart.
            this.overwriteStoreInfoJson(storeInfoList)

            // then, create the store itself
            val store = StoreImpl(
                id = newStoreInfo.storeId,
                name = newStoreInfo.storeName,
                retainOldVersions = newStoreInfo.retainOldVersions,
                validFrom = newStoreInfo.validFrom,
                validTo = newStoreInfo.validTo,
                createdByTransactionId = transaction.id,
                directory = directory,
                blockReadMode = this.blockReadMode,
                mergeService = this.mergeService,
                blockCache = this.blockCacheManager.getBlockCache(storeId),
                driverFactory = this.driverFactory,
                newFileSettings = this.newFileSettings,
            )

            this.registerStoreInCaches(store)
            return store
        }
    }

    override fun renameStore(transaction: ChronoStoreTransaction, oldName: String, newName: String): Boolean {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.lock.write {
            assertInitialized()
            val store = this.getStoreByNameOrNull(transaction, oldName)
                ?: return false  // not found

            return this.renameStoreInternal(store, newName)
        }
    }

    override fun renameStore(transaction: ChronoStoreTransaction, storeId: StoreId, newName: String): Boolean {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.lock.write {
            assertInitialized()

            val store = this.getStoreByIdOrNull(transaction, storeId)
                ?: return false  // not found

            return this.renameStoreInternal(store, newName)
        }
    }

    private fun renameStoreInternal(store: Store, newName: String): Boolean {
        this.lock.write {
            assertInitialized()

            require(!storesByName.containsKey(newName)) {
                " Cannot rename store '${store.name}' to '${newName}': There already exists a store with name '${newName}'!"
            }

            val oldName = store.name

            val storeInfoList = mutableListOf<StoreInfo>()
            this.storesById.values.asSequence().map { it.storeInfo }.toCollection(storeInfoList)
            storeInfoList.replaceAll { storeInfo ->
                if (storeInfo.storeId == store.id) {
                    storeInfo.copy(storeName = newName)
                } else {
                    storeInfo
                }
            }
            this.overwriteStoreInfoJson(storeInfoList)

            store.name = newName

            storesByName.remove(oldName)
            storesByName[newName] = store

            return true
        }
    }

    override fun deleteStoreByName(transaction: ChronoStoreTransaction, name: String): Boolean {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.lock.write {
            assertInitialized()
            val store = this.getStoreByNameOrNull(transaction, name)
                ?: return false
            return this.deleteStoreInternal(store)
        }
    }

    override fun deleteStoreById(transaction: ChronoStoreTransaction, storeId: StoreId): Boolean {
        check(this.isOpen) { DB_ALREADY_CLOSED }
        this.lock.write {
            assertInitialized()
            val store = this.getStoreByIdOrNull(transaction, storeId)
                ?: return false
            return this.deleteStoreInternal(store)
        }
    }

    private fun deleteStoreInternal(store: Store): Boolean {
        check(this.isOpen) { "This database has already been closed." }
        this.lock.write {
            val storeInfoList = this.storesById.values.asSequence()
                .filter { it.id != store.id }
                .map { it.storeInfo }
                .toList()

            this.overwriteStoreInfoJson(storeInfoList)
            this.storesById.remove(store.id)
            this.storesByName.remove(store.name)
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
                store.tree.performGarbageCollection(store.name, taskMonitor)
            }
        }
        monitor.reportDone()
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
                return JsonUtil.readJsonAsObject(input)
            }
        }
    }

    private fun registerStoreInCaches(store: Store) {
        this.lock.write {
            this.storesById[store.id] = store
            this.storesByName[store.name] = store
        }
    }

    private fun assertInitialized() {
        check(this.initialized) { "The StoreManager has not yet been initialized!" }
    }

    private val Store.storeInfo: StoreInfo
        get() = StoreInfo(id, name, retainOldVersions, validFrom, validTo, createdByTransactionId)

    private fun Store.isVisibleFor(transaction: ChronoStoreTransaction): Boolean {
        val validFrom = this.validFrom
        val validTo = this.validTo ?: Timestamp.MAX_VALUE
        val txTimestamp = transaction.lastVisibleTimestamp
        val createdByThisTransaction = this.createdByTransactionId == transaction.id
        return (validFrom < txTimestamp || createdByThisTransaction) && (txTimestamp < validTo || this.retainOldVersions)
    }


    private data class StoreInfo(
        val storeId: StoreId,
        val storeName: String,
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