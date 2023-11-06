package org.chronos.chronostore.io.vfs.disk

import org.chronos.chronostore.util.IOExtensions.sync
import org.chronos.chronostore.util.stream.UnclosableOutputStream.Companion.unclosable
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

enum class FileSyncMode {

    /**
     * Disables any form of synchronization between the file and the disk.
     *
     * **DO NOT USE THIS OPTION** unless you have other measures in place to ensure
     * that transient writes are always fully written. Using this option without
     * proper measures in the deployment may cause data loss or data corruption
     * on power outage and on process kills.
     */
    NO_SYNC {

        override fun <T> performWriteAppend(target: File, action: (OutputStream) -> T): T {
            return FileOutputStream(target, true).use(action)
        }

    },

    /** Uses `fsync()` to flush writes to disk. Slowest method, but should be supported on all file systems. */
    FULL_FSYNC {

        override fun <T> performWriteAppend(target: File, action: (OutputStream) -> T): T {
            val outputStream = FileOutputStream(target, true)
            try {
                val bufferedOutputStream = outputStream.unclosable().buffered()
                val result = action(bufferedOutputStream)
                bufferedOutputStream.flush()
                return result
            } finally {
                outputStream.flush()
                outputStream.sync(target)
                outputStream.close()
            }
        }

    },

    /** Uses [StandardOpenOption.SYNC] to flush writes to disk. Faster than [FULL_FSYNC], but may not be available on all file systems. */
    CHANNEL_SYNC {

        override fun <T> performWriteAppend(target: File, action: (OutputStream) -> T): T {
            FileChannel.open(
                target.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.WRITE,
                StandardOpenOption.SYNC,
            ).use { channel ->
                channel.force(false)
                Channels.newOutputStream(channel).buffered().use { outputStream ->
                    val result = action(outputStream)
                    outputStream.flush()
                    return result
                }
            }
        }

    },

    /** Uses [StandardOpenOption.DSYNC] to flush writes to disk. Faster than [CHANNEL_SYNC], but may not be available on all file systems. */
    CHANNEL_DATASYNC {

        override fun <T> performWriteAppend(target: File, action: (OutputStream) -> T): T {
            FileChannel.open(
                target.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.WRITE,
                StandardOpenOption.DSYNC,
            ).use { channel ->
                channel.force(false)
                Channels.newOutputStream(channel).buffered().use { outputStream ->
                    val result = action(outputStream)
                    outputStream.flush()
                    return result
                }
            }
        }

    },

    ;

    abstract fun <T> performWriteAppend(target: File, action: (OutputStream) -> T): T


}