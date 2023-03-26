package org.example.dbfromzero.io.vfs.inmemory

import org.example.dbfromzero.io.vfs.VirtualFile
import java.io.File
import java.io.InputStream

open class InMemoryVirtualFile(
    final override val parent: InMemoryVirtualDirectory,
    final override val name: String
) : VirtualFile, InMemoryVirtualFileSystemElement {

    final override val fileSystem: InMemoryFileSystem = parent.fileSystem
    final override val path: String = this.parent.path + File.separator + this.name

    override fun exists(): Boolean {
        return this.fileSystem.isExistingPath(this.path)
    }

    override val length: Long
        get() = this.fileSystem.getFileLength(this.path)


    override fun createInputStream(): InputStream {
        val content = this.fileSystem.getFileContent(this.path)
        return content.createInputStream()
    }

}