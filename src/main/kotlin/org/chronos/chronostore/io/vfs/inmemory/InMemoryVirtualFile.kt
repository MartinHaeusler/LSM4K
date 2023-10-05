package org.chronos.chronostore.io.vfs.inmemory

import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.util.bytes.Bytes
import java.io.File
import java.io.InputStream

open class InMemoryVirtualFile(
    final override val parent: InMemoryVirtualDirectory?,
    final override val fileSystem: InMemoryVirtualFileSystem,
    final override val name: String,
) : VirtualFile, InMemoryVirtualFileSystemElement {

    final override val path: String
        get() {
            val parentPath = this.parent?.path
                ?: return InMemoryVirtualFileSystem.PATH_PREFIX + this.name

            return "${parentPath}${File.separator}${this.name}"
        }

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
        if(offset + bytesToRead > content.size){
            // requested range goes beyond end of the array.
            // The "slice(...)"  method used below would actually
            // allow this and just return the bytes which are
            // present, but the definition of "readAtOffsetOrNull(...)"
            // specifies that NULL must be returned if we can't
            // read all of the requested bytes.
            return null
        }
        return content.slice(offset, bytesToRead)
    }

    override fun toString(): String {
        return "File[${this.path}]"
    }

}