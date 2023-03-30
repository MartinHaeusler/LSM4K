package org.chronos.chronostore.io.vfs.inmemory

import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.util.Bytes
import java.io.File
import java.io.InputStream

open class InMemoryVirtualFile(
    final override val parent: InMemoryVirtualDirectory?,
    final override val fileSystem: InMemoryVirtualFileSystem,
    final override val name: String
) : VirtualFile, InMemoryVirtualFileSystemElement {

    final override val path: String = "${this.parent?.path ?: InMemoryVirtualFileSystem.PATH_PREFIX}${File.separator}${this.name}"

    override fun exists(): Boolean {
        return this.fileSystem.isExistingPath(this.path)
    }

    override val length: Long
        get() = this.fileSystem.getFileLength(this.path)


    override fun createInputStream(): InputStream {
        val content = this.fileSystem.getFileContent(this.path)
        return content.createInputStream()
    }

    fun readAtOffsetOrNull(offset: Int, bytesToRead: Int): Bytes? {
        require(offset >= 0) { "Argument 'offset' must not be negative, but got: ${offset}" }
        require(bytesToRead >= 0) { "Argument 'bytesToRead' mut not be negative, but got: ${bytesToRead}" }
        val content = this.fileSystem.getFileContent(this.path)
        // we have to convert to long here in order to avoid numeric overflow!
        if ((offset.toLong() + bytesToRead.toLong()) > content.size) {
            // file has not enough bytes
            return null
        }
        if(bytesToRead == 0){
            return Bytes.EMPTY
        }
        val buffer = ByteArray(bytesToRead)
        System.arraycopy(
            /* src = */ content,
            /* srcPos = */ offset,
            /* dest = */ buffer,
            /* destPos = */ 0,
            /* length = */ buffer.size
        )
        return Bytes(buffer)
    }

    override fun toString(): String {
        return "File[${this.path}]"
    }

}