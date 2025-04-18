package org.chronos.chronostore.io.fileaccess

import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFile
import org.chronos.chronostore.io.vfs.inmemory.InMemoryVirtualFile
import org.chronos.chronostore.util.bytes.Bytes
import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

class FileChannelDriver(
    private val file: File
) : RandomFileAccessDriver {

    // TODO [PERFORMANCE]: Attempt to use "ExtendedOpenOption.DIRECT". This requires aligning the memory to the file system page size.
    // See: https://stackoverflow.com/a/73625490/3094906
    private val channel: FileChannel = FileChannel.open(this.file.toPath(), StandardOpenOption.READ)

    private var closed = false

    override val fileSize: Long
        get() = this.file.length()

    override val filePath: String
        get() = this.file.absolutePath

    override fun readBytesOrNull(offset: Long, bytesToRead: Int): Bytes? {
        require(offset >= 0) { "Argument 'offset' must not be negative, but got: ${offset}" }
        require(bytesToRead >= 0) { "Argument 'bytesToRead' must not be negative, but got: ${bytesToRead}" }
        channel.position(offset)
        val buffer = ByteBuffer.allocate(bytesToRead)
        val bytesRead = this.channel.read(buffer)
        return if (bytesRead < bytesToRead) {
            // file doesn't contain enough bytes.
            null
        } else {
            Bytes.wrap(buffer.array())
        }
    }

    override fun copy(): FileChannelDriver {
        return FileChannelDriver(this.file)
    }

    override fun toString(): String {
        return "FileChannelDriver[${this.file.path}]"
    }

    override fun close() {
        if (this.closed) {
            return
        }
        this.closed = true
        this.channel.close()
    }

    data object Factory : RandomFileAccessDriverFactory {

        override fun createDriver(file: VirtualFile): RandomFileAccessDriver {
            return when (file) {
                is DiskBasedVirtualFile -> FileChannelDriver(file.fileOnDisk)
                // fall back to in-memory driver
                is InMemoryVirtualFile -> InMemoryFileDriver(file)
                else -> throw IllegalArgumentException("Unknown subclass of ${VirtualFile::class.simpleName}: ${file::class.qualifiedName}")
            }
        }

    }

}