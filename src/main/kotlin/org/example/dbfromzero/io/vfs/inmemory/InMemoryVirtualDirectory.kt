package org.example.dbfromzero.io.vfs.inmemory

import org.example.dbfromzero.io.vfs.VirtualDirectory
import org.example.dbfromzero.io.vfs.VirtualFile
import org.example.dbfromzero.io.vfs.VirtualReadWriteFile
import java.io.File

class InMemoryVirtualDirectory : VirtualDirectory, InMemoryVirtualFileSystemElement {

    override val fileSystem: InMemoryFileSystem
    override val parent: InMemoryVirtualDirectory?
    override val path: String
    override val name: String

    constructor(parent: InMemoryVirtualDirectory, name: String) {
        this.fileSystem = parent.fileSystem
        this.parent = parent
        this.name = name
        this.path = "<memory>://${parent.path}${File.separator}${this.name}"
    }

    constructor(fileSystem: InMemoryFileSystem, name: String) {
        this.fileSystem = fileSystem
        this.parent = null
        this.name = name
        this.path = this.name
    }

    override fun exists(): Boolean {
        return this.fileSystem.isExistingPath(this.path)
    }

    override fun list(): List<String> {
        return this.fileSystem.listChildrenOfPath(this.path)
    }

    override fun mkdirs() {
        this.fileSystem.mkdirs(this.path)
    }

    override fun clear() {
        for (child in this.fileSystem.listChildrenOfPath(this.path)) {
            val childPath = this.path + File.separator + child
            if (this.fileSystem.isFile(childPath)) {
                this.fileSystem.delete(childPath)
            }
            if (this.fileSystem.isDirectory(childPath)) {
                // recursively clear the content of the directory
                this.fileSystem.directory(childPath).clear()
                // clear the directory itself
                this.fileSystem.delete(childPath)
            }
        }
    }

    override fun file(name: String): VirtualReadWriteFile {
        return InMemoryVirtualReadWriteFile(this, name)
    }

    override fun directory(name: String): VirtualDirectory {
        return InMemoryVirtualDirectory(this, name)
    }


}