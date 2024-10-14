package org.chronos.chronostore.lsm.merge.tasks

import org.chronos.chronostore.api.Store
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.api.compaction.LeveledCompactionStrategy
import org.chronos.chronostore.api.compaction.TieredCompactionStrategy
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.forEachWithMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.impl.StoreManagerImpl
import org.chronos.chronostore.lsm.merge.algorithms.LeveledCompaction
import org.chronos.chronostore.lsm.merge.algorithms.TieredCompaction
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
            monitor.reportStarted("Compacting ChronoStore")
            val allStores = (this.storeManager as StoreManagerImpl).getAllStoresInternal()
            monitor.forEachWithMonitor(1.0, "Compacting Store", allStores) { taskMonitor, store ->
                mergeFilesIn(store, major, taskMonitor)
            }
        }
    }

    private fun mergeFilesIn(store: Store, major: Boolean, monitor: TaskMonitor) {
        val compactionNote = if (major) {
            "(Major Compaction)"
        } else {
            "(Minor Compaction)"
        }
        monitor.reportStarted("Compacting Store '${store.storeId}' ${compactionNote}")
        if (major) {
            this.performMajorCompaction(store, monitor)
        } else {
            this.performMinorCompaction(store, monitor)
        }
//        val lsmTree = (store as StoreImpl).tree
//        val files = lsmTree.allFiles
//        val filesToMerge = selectFiles(major, files)
//        if(filesToMerge.size <= 1){
//            return // no files need merging in this store
//        }
//
//        monitor.reportProgress(0.2)
//        val fileIndices = filesToMerge.asSequence().map { files.indexOf(it) }.toSet()
//        monitor.subTaskWithMonitor(0.8) { mergeMonitor ->
//            lsmTree.mergeFiles(fileIndices, mergeMonitor)
//        }
    }

    private fun performMajorCompaction(store: Store, monitor: TaskMonitor) {

    }

    private fun performMinorCompaction(store: Store, monitor: TaskMonitor) {
        val storeMetadata = manifestFile.getManifest().getStore(store.storeId)
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
        store: Store,
        storeMetadata: StoreMetadata,
        compactionStrategy: TieredCompactionStrategy,
        monitor: TaskMonitor,
    ) {
        val compaction = TieredCompaction(
            manifestFile = this.manifestFile,
            store = store,
            storeMetadata = storeMetadata,
            configuration = compactionStrategy,
            monitor = monitor
        )
        compaction.runCompaction()
    }

    private fun performMinorLeveledCompaction(
        store: Store,
        storeMetadata: StoreMetadata,
        compactionStrategy: LeveledCompactionStrategy,
        monitor: TaskMonitor,
    ) {
        val compaction = LeveledCompaction(
            manifestFile = this.manifestFile,
            store = store,
            storeMetadata = storeMetadata,
            configuration = compactionStrategy,
            monitor = monitor
        )
        compaction.runCompaction()
    }


}