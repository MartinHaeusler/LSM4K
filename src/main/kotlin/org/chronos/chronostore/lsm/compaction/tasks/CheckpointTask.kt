package org.chronos.chronostore.lsm.compaction.tasks

import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.mainTask
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.subTask
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.impl.Killswitch
import org.chronos.chronostore.impl.StoreManagerImpl
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.wal.WriteAheadLog
import java.util.concurrent.CompletableFuture

/**
 * A [CheckpointTask] is an async task which is run periodically for "cleanup duty".
 *
 * It has multiple sub-tasks:
 *
 * - **It flushes memtables of all LSM trees to disk.**
 *
 *     This helps to avoid LSM trees with low write activity to indefinitely hold their data in memory.
 *
 * - **It drops outdated entries from the Write-Ahead-Log.**
 *
 *     Entries which are already persistent in the LSM trees on-disk are removed from the Write-Ahead-Log
 *     in order to reduce its disk footprint and speed up the next startup of the store.
 *
 * - **It computes the checksums for the Write-Ahead-Log files.**
 *
 *     This allows to validate the Write-Ahead-Log integrity on the next startup of the store.
 *
 * - **It shortens the manifest file by collapsing all operations into a single checkpoint operation.**
 *
 *     Manifest operations are appended as management operations occur on the store (e.g. new store creation,
 *     flush operations, compaction operations, etc.). These operations accumulate over time which may cause
 *     the manifest file to grow to considerable sizes. This task computes the resulting final state of the
 *     manifest after applying all operations on it and replaces the additive operations by a single "reset"
 *     operation which contains the entire state of the store.
 */
class CheckpointTask(
    private val writeAheadLog: WriteAheadLog,
    private val storeManager: StoreManager,
    private val manifestFile: ManifestFile,
    private val killswitch: Killswitch,
) : AsyncTask {

    companion object {

        /** The minimum number of operations to be contained in the manifest before a checkpoint is done. */
        private const val MINIMUM_NUMBER_OF_MANIFEST_OPERATIONS_BEFORE_CHECKPOINT = 1000

        private val log = KotlinLogging.logger {}
    }

    override val name: String
        get() = "Checkpoint"

    override fun run(monitor: TaskMonitor) = monitor.mainTask("WAL Compaction") {
        try {
            log.info { "Checkpoint Task started" }
            val timeBefore = System.currentTimeMillis()

            // check which WAL file is the current one (it doesn't matter if other files get added later while we're busy)
            val currentWalFileSequenceNumber = this.writeAheadLog.getLatestFileSequenceNumber()

            // flush all trees
            monitor.subTask(0.4, "Flush in-memory changes to disk") {
                val lsmTrees = (this.storeManager as StoreManagerImpl).getAllLsmTreesAdmin()
                CompletableFuture.allOf(
                    *lsmTrees.map { it.scheduleMemtableFlush() }.toTypedArray()
                ).join()
            }

            // attempt to shorten the WAL by dropping all files that are no longer needed.
            // we can do this safely because we've just enforced a flush operation on all LSM trees,
            // so ALL data is currently persisted on disk.
            monitor.subTask(0.3, "Shortening Write-Ahead-Log") {
                if (currentWalFileSequenceNumber != null) {
                    this.writeAheadLog.deleteWalFilesWithSequenceNumberLowerThan(currentWalFileSequenceNumber)
                }
            }
            // then, compute the checksums for all remaining WAL files which have been completed
            monitor.subTask(0.1, "Generating Checksums for Write-Ahead-Log files") {
                this.writeAheadLog.generateChecksumsForCompletedFiles()
            }

            // shorten the WAL if it got too long by folding all operations into a single checkpoint operation
            monitor.subTask(0.2, "Creating checkpoint in manifest") {
                this.manifestFile.createCheckpointIfNumberOfOperationsExceeds(MINIMUM_NUMBER_OF_MANIFEST_OPERATIONS_BEFORE_CHECKPOINT)
            }

            val timeAfter = System.currentTimeMillis()
            log.info { "Checkpoint Task completed in ${timeAfter - timeBefore}ms." }
        } catch (t: Throwable) {
            killswitch.panic("An unexpected error occurred during the Checkpoint task.", t)
            throw t
        }
        monitor.reportDone()
    }

}