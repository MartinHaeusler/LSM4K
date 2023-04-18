package org.chronos.chronostore.impl

import mu.KotlinLogging
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.datablock.BlockReadMode
import org.chronos.chronostore.io.structure.ChronoStoreStructure.STORE_DIR_PREFIX
import org.chronos.chronostore.io.structure.ChronoStoreStructure.STORE_INFO_FILE_NAME
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.lsm.cache.BlockCacheManager
import org.chronos.chronostore.lsm.MergeStrategy
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.json.JsonUtil
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class StoreManagerImpl(
    private val vfs: VirtualFileSystem,
    private val blockCache: BlockCacheManager,
    private val blockReadMode: BlockReadMode,
    private val mergeStrategy: MergeStrategy,
    private val driverFactory: RandomFileAccessDriverFactory,
) : StoreManager {

    companion object {

        private val log = KotlinLogging.logger {}

    }

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
                        directory = vfs.directory(STORE_DIR_PREFIX + storeInfo.storeId),
                        blockReadMode = this.blockReadMode,
                        mergeStrategy = this.mergeStrategy,
                        blockCache = this.blockCache.getBlockCache(storeInfo.storeId),
                        driverFactory = this.driverFactory
                    )
                    this.registerStoreInCaches(store)
                }
            }
            this.initialized = true
        }
    }

    override fun getStoreByNameOrNull(name: String): Store? {
        this.lock.read {
            assertInitialized()
            return this.storesByName[name]
        }
    }

    override fun getStoreByIdOrNull(storeId: StoreId): Store? {
        this.lock.read {
            assertInitialized()
            return this.storesById[storeId]
        }
    }

    override fun createNewStore(name: String, versioned: Boolean): Store {
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
            this.storesById.values.asSequence().map { StoreInfo(it.id, it.name, it.retainOldVersions) }.toCollection(storeInfoList)
            storeInfoList.add(StoreInfo(storeId, name, versioned))

            // first, write the store info into our JSON file. This will ensure that
            // the store still exists upon the next database restart.
            this.overwriteStoreInfoJson(storeInfoList)

            // then, create the store itself
            val store = StoreImpl(
                id = storeId,
                name = name,
                retainOldVersions = versioned,
                directory = directory,
                blockReadMode = this.blockReadMode,
                mergeStrategy = this.mergeStrategy,
                blockCache = this.blockCache.getBlockCache(storeId),
                driverFactory = this.driverFactory
            )

            this.registerStoreInCaches(store)
            return store
        }
    }

    override fun renameStore(oldName: String, newName: String): Boolean {
        this.lock.write {
            assertInitialized()
            val store = this.getStoreByNameOrNull(oldName)
                ?: return false  // not found

            return this.renameStoreInternal(store, newName)
        }
    }

    override fun renameStore(storeId: StoreId, newName: String): Boolean {
        this.lock.write {
            assertInitialized()

            val store = this.getStoreByIdOrNull(storeId)
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
            this.storesById.values.asSequence().map { StoreInfo(it.id, it.name, it.retainOldVersions) }.toCollection(storeInfoList)
            storeInfoList.replaceAll { storeInfo ->
                if (storeInfo.storeId == store.id) {
                    StoreInfo(storeInfo.storeId, newName, storeInfo.retainOldVersions)
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

    override fun deleteStoreByName(name: String): Boolean {
        this.lock.write {
            assertInitialized()
            val store = this.getStoreByNameOrNull(name)
                ?: return false
            return this.deleteStoreInternal(store)
        }
    }

    override fun deleteStoreById(storeId: StoreId): Boolean {
        this.lock.write {
            assertInitialized()
            val store = this.getStoreByIdOrNull(storeId)
                ?: return false
            return this.deleteStoreInternal(store)
        }
    }

    private fun deleteStoreInternal(store: Store): Boolean {
        this.lock.write {
            val storeInfoList = this.storesById.values.asSequence()
                .filter { it.id != store.id }
                .map { StoreInfo(it.id, it.name, it.retainOldVersions) }
                .toList()

            this.overwriteStoreInfoJson(storeInfoList)
            this.storesById.remove(store.id)
            this.storesByName.remove(store.name)
            store.directory.clear()
            store.directory.delete()
            return true
        }
    }

    override fun getAllStores(): List<Store> {
        this.lock.read {
            this.assertInitialized()
            return this.storesById.values.toList()
        }
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

    private class StoreInfo(
        val storeId: StoreId,
        val storeName: String,
        val retainOldVersions: Boolean,
    )

}