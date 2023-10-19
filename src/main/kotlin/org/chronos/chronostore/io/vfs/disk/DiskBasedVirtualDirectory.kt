package org.chronos.chronostore.io.vfs.disk

import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.io.vfs.VirtualFileSystemElement
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import java.io.File
import java.nio.file.Files

class DiskBasedVirtualDirectory(
    override val parent: DiskBasedVirtualDirectory?,
    private val file: File,
) : VirtualDirectory {


    override fun list(): List<String> {
        return this.file.list()?.asList() ?: emptyList()
    }

    override fun listElements(): List<VirtualFileSystemElement> {
        return this.file.listFiles()?.map {
            if (it.isFile) {
                DiskBasedVirtualFile(this, it)
            } else {
                DiskBasedVirtualDirectory(this, it)
            }
        } ?: emptyList()
    }

    override fun mkdirs() {
        if (!this.file.exists()) {
            this.file.mkdirs()
        }
    }

    override fun clear() {
        for (file in this.file.listFiles() ?: emptyArray()) {
            file.deleteRecursively()
        }
    }

    override fun delete() {
        Files.delete(this.file.toPath())
    }

    override fun file(name: String): VirtualReadWriteFile {
        return DiskBasedVirtualReadWriteFile(this, File(file, name))
    }

    override fun directory(name: String): VirtualDirectory {
        return DiskBasedVirtualDirectory(this, File(this.file, name))
    }

    override val name: String
        get() = this.file.name

    override val path: String
        get() = this.file.absolutePath

    override fun exists(): Boolean {
        return this.file.exists() && this.file.isDirectory
    }

    override fun toString(): String {
        return "Dir[${this.path}]"
    }

}