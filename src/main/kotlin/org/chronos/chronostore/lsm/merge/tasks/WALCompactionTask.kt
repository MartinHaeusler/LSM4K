package org.chronos.chronostore.lsm.merge.tasks

import org.chronos.chronostore.api.Store
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.wal.WriteAheadLogTransaction
import org.chronos.chronostore.wal.WriteAheadLog

class WALCompactionTask(
    private val writeAheadLog: WriteAheadLog,
    private val storeManager: StoreManager,
) : AsyncTask {

    override val name: String
        get() = "WAL Compaction"

    override fun run(monitor: TaskMonitor) {
        monitor.reportStarted("WAL Compaction")
        val lowWatermark = this.storeManager.getLowWatermarkTimestamp()
        this.writeAheadLog.shorten(lowWatermark)
        monitor.reportDone()
    }

    private fun isFullyPersisted(
        transaction: WriteAheadLogTransaction,
        nameToStore: Map<StoreId, Store>
    ): Boolean {
        // a transaction is fully persisted if all involved stores...
        // - ... EITHER don't exist anymore
        // - ... OR contain the transaction timestamp in their persisted files
        return transaction.storeIdToCommands.keys.asSequence()
            .mapNotNull { nameToStore[it] }
            .filterIsInstance<StoreImpl>()
            .all { store -> hasPersistedTransactionTimestamp(store.tree, transaction.commitTimestamp) }
    }

    private fun hasPersistedTransactionTimestamp(lsmTree: LSMTree, commitTimestamp: Long): Boolean {
        // the transaction timestamp is persisted if ANY of the files contains it
        return lsmTree.allFiles.any { lsmTreeFile ->
            val maxTimestamp = lsmTreeFile.header.metaData.maxTimestamp ?: 0
            maxTimestamp >= commitTimestamp
        }
    }

}