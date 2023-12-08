package org.chronos.chronostore.checkpoint

import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class CheckpointManager(
    private val directory: VirtualDirectory,
    private val maxCheckpointFiles: Int,
) {

    private val lock = ReentrantReadWriteLock(true)

    // we use a LinkedList here on purpose. We expect it to be short
    // (bounded by maxCheckpointFiles) and the most frequent operations
    // are removeFirst (catastrophic on array lists) and getLast.
    private val checkpointFiles = LinkedList<CheckpointFile>()

    init {
        require(this.maxCheckpointFiles > 0) {
            "Precondition violation - argument 'maxCheckpointFiles' (${maxCheckpointFiles}) must be greater than 0!"
        }

        if(!this.directory.exists()){
            this.directory.mkdirs()
        }

        this.checkpointFiles += this.directory.listFiles()
            .mapNotNull { CheckpointFile.readOrNull(it as VirtualReadWriteFile) }
            .sortedBy { it.wallClockTime }

        // make sure that all checkpoint files have proper wall clock timestamps
        // which are less than the system time.
        val now = System.currentTimeMillis()
        check(checkpointFiles.none { it.wallClockTime > now }) {
            val offendingTimestamp = checkpointFiles.first { it.wallClockTime > now }.wallClockTime
            "Failed to load ChronoStore: There is a checkpoint file which has a" +
                " higher timestamp (${offendingTimestamp}) than the current system clock (${now})." +
                " This must not happen and is likely caused by a wrong system clock setting."
        }
    }

    fun saveCheckpoint(checkpointData: CheckpointData) {
        this.lock.write {
            val wallClockTime = System.currentTimeMillis()
            // safeguard, this should never happen!
            check(wallClockTime > (this.checkpointFiles.lastOrNull()?.wallClockTime ?: -1)) {
                "The last checkpoint was at ${this.checkpointFiles.last().wallClockTime} but the system clock is at ${wallClockTime}!"
            }
            val targetFile = this.directory.file(CheckpointFile.createFileName(wallClockTime))
            this.checkpointFiles += CheckpointFile.write(targetFile, checkpointData)
            // ensure that we don't have too many files
            while (this.checkpointFiles.size > this.maxCheckpointFiles) {
                val oldestCheckpointFile = this.checkpointFiles.poll()
                oldestCheckpointFile.delete()
            }
        }
    }

    fun getLatestCheckpoint(): CheckpointData? {
        this.lock.read {
            return this.checkpointFiles.lastOrNull()?.checkpointData
        }
    }


}