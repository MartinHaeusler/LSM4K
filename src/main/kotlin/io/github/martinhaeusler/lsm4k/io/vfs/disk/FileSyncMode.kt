package io.github.martinhaeusler.lsm4k.io.vfs.disk

import io.github.martinhaeusler.lsm4k.util.IOExtensions.sync
import io.github.martinhaeusler.lsm4k.util.stream.CloseHandlerOutputStream.Companion.onClose
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

/**
 * The [FileSyncMode] determines how the data that is being written to a file is synced to disk.
 */
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

        override fun createOutputStream(target: File, append: Boolean): OutputStream {
            return FileOutputStream(target, append).buffered()
        }

        override fun sync(channel: FileChannel) {
            // nothing to do
        }

    },

    /** Uses `fsync()` to flush writes to disk. Slowest method, but should be supported on all file systems. */
    FULL_FSYNC {

        override fun createOutputStream(target: File, append: Boolean): OutputStream {
            val outputStream = FileOutputStream(target, append)
            val bufferedStream = outputStream.buffered()
            return bufferedStream.onClose {
                bufferedStream.flush()
                outputStream.sync(target)
                outputStream.close()
            }
        }

        override fun sync(channel: FileChannel) {
            channel.force(true)
        }

    },

    /** Uses [StandardOpenOption.SYNC] to flush writes to disk. Faster than [FULL_FSYNC], but may not be available on all file systems. */
    CHANNEL_SYNC {

        private val writeSettings = arrayOf(StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        private val appendSettings = arrayOf(*writeSettings, StandardOpenOption.APPEND)

        override fun createOutputStream(target: File, append: Boolean): OutputStream {
            val settings = if (append) {
                appendSettings
            } else {
                writeSettings
            }

            val channel = FileChannel.open(target.toPath(), *settings)
            return Channels.newOutputStream(channel)
                .onClose { this.sync(channel) }
                .buffered()
        }

        override fun sync(channel: FileChannel) {
            channel.force(true)
        }
    },

    /** Uses [StandardOpenOption.DSYNC] to flush writes to disk. Faster than [CHANNEL_SYNC], but may not be available on all file systems. */
    CHANNEL_DATASYNC {


        private val writeSettings = arrayOf(StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        private val appendSettings = arrayOf(*writeSettings, StandardOpenOption.APPEND)

        override fun createOutputStream(target: File, append: Boolean): OutputStream {
            val settings = if (append) {
                appendSettings
            } else {
                writeSettings
            }

            val channel = FileChannel.open(target.toPath(), *settings)
            return Channels.newOutputStream(channel)
                .onClose { this.sync(channel) }
                .buffered()
        }

        override fun sync(channel: FileChannel) {
            channel.force(false)
        }
    },

    ;

    fun <T> writeAppend(target: File, action: (OutputStream) -> T): T {
        return this.createOutputStream(target, append = true).use(action)
    }

    /**
     * Creates an [OutputStream] that carries out the write operations according to the [FileSyncMode].
     *
     * Users may assume that the returned stream is already buffered; no further buffering is necessary.
     *
     * @param target The file which should receive the data. If it doesn't exist yet, it will be created.
     * @param append Use `true` to append new data to the existing file. Use `false` to overwrite the existing data.
     */
    abstract fun createOutputStream(target: File, append: Boolean): OutputStream

    abstract fun sync(channel: FileChannel)

}