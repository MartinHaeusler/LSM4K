package org.chronos.chronostore.lsm.compaction.tasks

import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.api.compaction.LeveledCompactionStrategy
import org.chronos.chronostore.api.compaction.TieredCompactionStrategy
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.forEachWithMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.mainTask
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTaskWithMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.impl.StoreManagerImpl
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.lsm.compaction.algorithms.FullCompactionTask
import org.chronos.chronostore.lsm.compaction.algorithms.LeveledCompactionTask
import org.chronos.chronostore.lsm.compaction.algorithms.TieredCompactionTask
import org.chronos.chronostore.lsm.compaction.model.StandardCompactableStore
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.manifest.StoreMetadata
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class CompactionTask(
    val storeManager: StoreManager,
    val manifestFile: ManifestFile,
) : AsyncTask {

    private val lock = ReentrantLock(true)

    override val name: String
        get() = "Compaction"


    override fun run(monitor: TaskMonitor) {
        // if we're coming from the async task executor, it's
        // always a major compaction.
        this.runMajor(monitor)
    }

    fun runMinor(monitor: TaskMonitor) {
        this.runInternal(monitor, major = false)
    }

    fun runMajor(monitor: TaskMonitor) {
        this.runInternal(monitor, major = true)
    }

    private fun runInternal(monitor: TaskMonitor, major: Boolean) {
        this.lock.withLock {
            monitor.mainTask("Compacting ChronoStore"){
                val allStores = (this.storeManager as StoreManagerImpl).getAllStoresInternal()
                monitor.forEachWithMonitor(1.0, "Compacting Store", allStores) { taskMonitor, store ->
                    mergeFilesIn(store as StoreImpl, major, taskMonitor)
                }
            }
        }
    }

    private fun mergeFilesIn(store: StoreImpl, major: Boolean, monitor: TaskMonitor) {
        val compactionNote = if (major) {
            "(Major Compaction)"
        } else {
            "(Minor Compaction)"
        }
        monitor.mainTask("Compacting Store '${store.storeId}' ${compactionNote}"){
            monitor.subTaskWithMonitor(1.0) { subMonitor ->
                if (major) {
                    this.performMajorCompaction(store, subMonitor)
                } else {
                    this.performMinorCompaction(store, subMonitor)
                }
            }
        }
    }

    private fun performMajorCompaction(store: StoreImpl, monitor: TaskMonitor) {
        val storeMetadata = this.manifestFile.getManifest().getStore(store.storeId)

        val compactableStore = StandardCompactableStore(store, storeMetadata)

        val compaction = FullCompactionTask(
            manifestFile = this.manifestFile,
            store = compactableStore,
        )
        compaction.runCompaction(monitor)
    }

    private fun performMinorCompaction(store: StoreImpl, monitor: TaskMonitor) {
        val storeMetadata = this.manifestFile.getManifest().getStore(store.storeId)
        when (val compactionStrategy = storeMetadata.compactionStrategy) {
            is LeveledCompactionStrategy -> this.performMinorLeveledCompaction(
                store = store,
                storeMetadata = storeMetadata,
                compactionStrategy = compactionStrategy,
                monitor = monitor,
            )

            is TieredCompactionStrategy -> this.performMinorTieredCompaction(
                store = store,
                storeMetadata = storeMetadata,
                compactionStrategy = compactionStrategy,
                monitor = monitor,
            )
        }
    }

    private fun performMinorTieredCompaction(
        store: StoreImpl,
        storeMetadata: StoreMetadata,
        compactionStrategy: TieredCompactionStrategy,
        monitor: TaskMonitor,
    ) {
        val compactableStore = StandardCompactableStore(store, storeMetadata)

        val compaction = TieredCompactionTask(
            manifestFile = this.manifestFile,
            store = compactableStore,
            configuration = compactionStrategy,
        )
        compaction.runCompaction(monitor)
    }

    private fun performMinorLeveledCompaction(
        store: StoreImpl,
        storeMetadata: StoreMetadata,
        compactionStrategy: LeveledCompactionStrategy,
        monitor: TaskMonitor,
    ) {
        val compactableStore = StandardCompactableStore(store, storeMetadata)


        val compaction = LeveledCompactionTask(
            manifestFile = this.manifestFile,
            store = compactableStore,
            configuration = compactionStrategy,
        )
        compaction.runCompaction(monitor)
    }


}