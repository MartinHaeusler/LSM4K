package org.chronos.chronostore.lsm.merge.tasks

import org.chronos.chronostore.api.MergeStrategy
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.forEachWithMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTaskWithMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.impl.StoreManagerImpl
import org.chronos.chronostore.impl.store.StoreImpl
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class CompactionTask(
    val storeManager: StoreManager,
    val mergeStrategy: MergeStrategy,
) : AsyncTask {

    private val lock = ReentrantLock(true)

    override val name: String
        get() = "Compaction"

    override fun run(monitor: TaskMonitor) {
        this.lock.withLock {
            monitor.reportStarted("Compacting ChronoStore")
            val allStores = (this.storeManager as StoreManagerImpl).getAllStoresInternal()
            monitor.forEachWithMonitor(1.0, "Compacting Store", allStores) { taskMonitor, store ->
                mergeFilesIn(store, taskMonitor)
            }
        }
    }

    private fun mergeFilesIn(store: Store, monitor: TaskMonitor) {
        monitor.reportStarted("Compacting Store '${store.name}'")
        val lsmTree = (store as StoreImpl).tree
        val files = lsmTree.allFiles
        val filesToMerge = this.mergeStrategy.selectFilesToMerge(files).takeIf { it.size > 1 }
            ?: return // no files need merging in this store

        monitor.reportProgress(0.2)
        val fileIndices = filesToMerge.asSequence().map { files.indexOf(it) }.toSet()
        monitor.subTaskWithMonitor(0.8) { mergeMonitor ->
            lsmTree.mergeFiles(fileIndices, mergeMonitor)
        }
    }
}