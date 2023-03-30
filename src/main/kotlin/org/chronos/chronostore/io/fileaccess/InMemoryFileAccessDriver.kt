package org.chronos.chronostore.io.fileaccess

import org.chronos.chronostore.io.vfs.inmemory.InMemoryVirtualFile
import org.chronos.chronostore.util.Bytes

class InMemoryFileAccessDriver(
    val inMemoryVirtualFile: InMemoryVirtualFile
) : RandomFileAccessDriver {

    private var closed = false

    override val size: Long
        get() = this.inMemoryVirtualFile.length

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

    override fun close() {
        this.closed = true
    }
}