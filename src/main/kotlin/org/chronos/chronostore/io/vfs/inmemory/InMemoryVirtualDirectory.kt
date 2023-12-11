package org.chronos.chronostore.io.vfs.inmemory

import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.io.vfs.VirtualFileSystemElement
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import java.io.File

class InMemoryVirtualDirectory : VirtualDirectory, InMemoryVirtualFileSystemElement {

    override val fileSystem: InMemoryVirtualFileSystem
    override val parent: InMemoryVirtualDirectory?
    override val path: String
    override val name: String

    constructor(parent: InMemoryVirtualDirectory, name: String) {
        this.fileSystem = parent.fileSystem
        this.parent = parent
        this.name = name
        this.path = "${parent.path}${File.separator}${this.name}"
    }

    constructor(fileSystem: InMemoryVirtualFileSystem, name: String) {
        this.fileSystem = fileSystem
        this.parent = null
        this.name = name
        this.path = "${InMemoryVirtualFileSystem.PATH_PREFIX}${this.name}"
    }

    override fun exists(): Boolean {
        return this.fileSystem.isExistingPath(this.path)
    }

    override fun list(): List<String> {
        return this.fileSystem.listChildrenOfPath(this.path)
    }

    override fun listElements(): List<VirtualFileSystemElement> {
        return this.list().map {
            if (this.fileSystem.isFile("${this.path}${File.separator}$it")) {
                this.file(it)
            } else {
                this.directory(it)
            }
        }
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

    override fun delete() {
        this.fileSystem.delete(this.path)
    }

    override fun file(name: String): VirtualReadWriteFile {
        return InMemoryVirtualReadWriteFile(this, this.fileSystem, name)
    }

    override fun directory(name: String): VirtualDirectory {
        return InMemoryVirtualDirectory(this, name)
    }

    override fun toString(): String {
        return "Dir[${this.path}]"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as InMemoryVirtualDirectory

        return path == other.path
    }

    override fun hashCode(): Int {
        return path.hashCode()
    }


}