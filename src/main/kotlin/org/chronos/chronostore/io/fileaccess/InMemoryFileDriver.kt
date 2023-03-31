package org.chronos.chronostore.io.fileaccess

import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFile
import org.chronos.chronostore.io.vfs.inmemory.InMemoryVirtualFile
import org.chronos.chronostore.util.Bytes

class InMemoryFileDriver(
    val inMemoryVirtualFile: InMemoryVirtualFile
) : RandomFileAccessDriver {

    private var closed = false

    override val size: Long by lazy {
        this.inMemoryVirtualFile.length
    }

    override val filePath: String
        get() = this.inMemoryVirtualFile.path

    override fun readBytesOrNull(offset: Long, bytesToRead: Int): Bytes? {
        require(offset >= 0) { "Argument 'offset' must not be negative, but got: ${offset}" }
        require(bytesToRead >= 0) { "Argument 'bytesToRead' must not be negative, but got: ${bytesToRead}" }
        check(!this.closed) { "This file access driver on '${inMemoryVirtualFile.path}' has already been closed!" }
        if (offset > Int.MAX_VALUE) {
            // we can't even convert this offset into an integer -> it's too big for sure!
            return null
        }
        return this.inMemoryVirtualFile.readAtOffsetOrNull(offset.toInt(), bytesToRead)
    }

    override fun copy(): InMemoryFileDriver {
        return InMemoryFileDriver(this.inMemoryVirtualFile)
    }

    override fun close() {
        this.closed = true
    }

    class Factory : RandomFileAccessDriverFactory {

        override fun createDriver(file: VirtualFile): RandomFileAccessDriver {
            return when (file) {
                is InMemoryVirtualFile -> InMemoryFileDriver(file)
                // fall back to file channel driver
                is DiskBasedVirtualFile -> FileChannelDriver(file.fileOnDisk)
                else -> throw IllegalArgumentException("Unknown subclass of ${VirtualFile::class.simpleName}: ${file::class.qualifiedName}")
            }
        }

    }

}