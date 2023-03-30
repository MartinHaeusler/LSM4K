package org.chronos.chronostore.io.vfs.inmemory

import org.chronos.chronostore.io.vfs.VirtualFile
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

    override fun toString(): String {
        return "File[${this.path}]"
    }

}