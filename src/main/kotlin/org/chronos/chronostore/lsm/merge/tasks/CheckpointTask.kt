package org.chronos.chronostore.lsm.merge.tasks

import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTask
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.checkpoint.CheckpointData
import org.chronos.chronostore.checkpoint.CheckpointManager
import org.chronos.chronostore.wal.WriteAheadLog

class CheckpointTask(
    private val writeAheadLog: WriteAheadLog,
    private val checkpointManager: CheckpointManager,
    private val storeManager: StoreManager,
) : AsyncTask {

    override val name: String
        get() = "Checkpoint"

    override fun run(monitor: TaskMonitor) {
        monitor.reportStarted("WAL Compaction")
        // first, attempt to shorten the WAL by dropping all files that are no longer needed.
        monitor.subTask(0.7, "Shortening Write-Ahead-Log") {
            if (this.writeAheadLog.needsToBeShortened()) {
                // save the checkpoint data first because it's faster than the shortening
                val lowWatermark = this.storeManager.getLowWatermarkTSN()
                this.checkpointManager.saveCheckpoint(CheckpointData(lowWatermark))
                // shorten the WAL by discarding old files based on the low watermark
                this.writeAheadLog.shorten(lowWatermark)
            }
        }
        // then, compute the checksums for all remaining WAL files which have been completed
        monitor.subTask(0.3, "Generating Checksums for Write-Ahead-Log files") {
            this.writeAheadLog.generateChecksumsForCompletedFiles()
        }
        monitor.reportDone()
    }

}