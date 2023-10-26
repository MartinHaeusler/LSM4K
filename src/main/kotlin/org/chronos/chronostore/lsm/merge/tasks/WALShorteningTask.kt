package org.chronos.chronostore.lsm.merge.tasks

import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.wal.WriteAheadLog

class WALShorteningTask(
    private val writeAheadLog: WriteAheadLog,
    private val storeManager: StoreManager,
) : AsyncTask {

    override val name: String
        get() = "WAL Compaction"

    override fun run(monitor: TaskMonitor) {
        monitor.reportStarted("WAL Compaction")
        if(this.writeAheadLog.needsToBeShortened()){
            val lowWatermark = this.storeManager.getLowWatermarkTimestamp()
            this.writeAheadLog.shorten(lowWatermark)
        }
        monitor.reportDone()
    }

}