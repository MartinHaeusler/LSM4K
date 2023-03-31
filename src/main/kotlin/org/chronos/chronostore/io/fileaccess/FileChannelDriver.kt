package org.chronos.chronostore.io.fileaccess

import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFile
import org.chronos.chronostore.io.vfs.inmemory.InMemoryVirtualFile
import org.chronos.chronostore.util.Bytes
import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

class FileChannelDriver(
    private val file: File
) : RandomFileAccessDriver {

    private val channel: FileChannel = FileChannel.open(this.file.toPath(), StandardOpenOption.READ)

    private var closed = false

    override val size: Long
        get() = this.file.length()

    override val filePath: String
        get() = this.file.absolutePath

    override fun readBytesOrNull(offset: Long, bytesToRead: Int): Bytes? {
        require(offset >= 0) { "Argument 'offset' must not be negative, but got: ${offset}" }
        require(bytesToRead >= 0) { "Argument 'bytesToRead' must not be negative, but got: ${bytesToRead}" }
        channel.position(offset)
        // TODO[Performance]: we might also try "ByteBuffer.allocateDirect(...)" here
        val buffer = ByteBuffer.allocate(bytesToRead)
        val bytesRead = this.channel.read(buffer)
        if (bytesRead < bytesToRead) {
            // file doesn't contain enough bytes.
            return null
        }
        return Bytes(buffer.array())
    }

    override fun copy(): FileChannelDriver {
        return FileChannelDriver(this.file)
    }

    override fun close() {
        if (this.closed) {
            return
        }
        this.closed = true
        this.channel.close()
    }

    class Factory : RandomFileAccessDriverFactory {

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