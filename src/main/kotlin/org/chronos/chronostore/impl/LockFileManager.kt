package org.chronos.chronostore.impl

import org.chronos.chronostore.api.exceptions.ChronoStoreException
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualReadWriteFile
import org.chronos.chronostore.util.ExceptionUtils
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Manages the exclusive process lock on the directory.
 *
 * Inspired by the "LockingManager" class in Jetbrains Xodus.
 */
class LockFileManager(
    val lockFile: VirtualReadWriteFile,
) {

    private val reentrantLock = ReentrantLock(true)
    private var actualFile: RandomAccessFile? = null
    private var lock: FileLock? = null

    fun lock(): Boolean {
        this.reentrantLock.withLock {
            if (this.lockFile !is DiskBasedVirtualReadWriteFile) {
                // in-memory files do not need (and do not support) locking.
                return false
            }

            if (this.lock != null) {
                // already locked
                return false
            }

            if (this.actualFile != null) {
                // we've already acquired the lock, nothing to do.
                return false
            }
            try {
                val lockFile = RandomAccessFile(this.lockFile.fileOnDisk, "rw")
                this.actualFile = lockFile
                val channel = lockFile.channel
                this.lock = channel.tryLock()
                if (this.lock != null) {
                    lockFile.setLength(0)
                    val pid = ProcessHandle.current().pid()
                    lockFile.writeLong(pid)
                    channel.force(false)
                } else {
                    this.unlockSafe()
                    throwFailedToLock()
                }
            } catch (e: IOException) {
                this.unlockSafe()
                throwFailedToLock(e)
            } catch (e: OverlappingFileLockException) {
                this.unlockSafe()
                throwFailedToLock(e)
            }
            return this.actualFile != null
        }
    }

    fun unlock(): Boolean {
        this.reentrantLock.withLock {
            val wasLocked = this.lock != null
            ExceptionUtils.runAndAggregateExceptions(
                { this.lock?.release() },
                { this.lock = null },
                { this.actualFile?.close() },
                { this.actualFile = null },
            )
            return wasLocked
        }
    }

    fun unlockSafe() {
        try {
            this.unlock()
        } catch (_: Exception) {
            // ignored
        }
    }

    private fun throwFailedToLock(e: Exception? = null): Nothing {
        throw ChronoStoreException(
            "Failed to acquire lock on directory '${this.lockFile.path}'." +
                " Chronostore requires exclusive access to its directory." +
                " Is there another instance of Chronostore still running?",
            e
        )
    }
}