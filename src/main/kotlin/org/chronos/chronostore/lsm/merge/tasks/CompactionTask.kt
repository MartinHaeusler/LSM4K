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
import org.chronos.chronostore.lsm.LSMTreeFile
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
        val lsmTree = (store as StoreImpl).tree
        val files = lsmTree.allFiles
        val filesToMerge = selectFiles(major, files)
        if(filesToMerge.size <= 1){
            return // no files need merging in this store
        }

        monitor.reportProgress(0.2)
        val fileIndices = filesToMerge.asSequence().map { files.indexOf(it) }.toSet()
        monitor.subTaskWithMonitor(0.8) { mergeMonitor ->
            lsmTree.mergeFiles(fileIndices, store.retainOldVersions, mergeMonitor)
        }
    }

    private fun selectFiles(major: Boolean, files: List<LSMTreeFile>): List<LSMTreeFile> {
        return if (major) {
            files
        } else {
            this.mergeStrategy.selectFilesToMerge(files)
        }
    }

}