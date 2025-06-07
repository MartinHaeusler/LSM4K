package io.github.martinhaeusler.lsm4k.io.vfs.disk

import io.github.martinhaeusler.lsm4k.io.vfs.VirtualDirectory
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFileSystemElement
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualReadWriteFile
import java.io.File
import java.nio.file.Files

class DiskBasedVirtualDirectory(
    override val parent: DiskBasedVirtualDirectory?,
    private val file: File,
    private val vfs: DiskBasedVirtualFileSystem,
) : VirtualDirectory {


    override fun list(): List<String> {
        return this.file.list()?.asList() ?: emptyList()
    }

    override fun listElements(): List<VirtualFileSystemElement> {
        return this.file.listFiles()?.map {
            if (it.isFile) {
                DiskBasedVirtualReadWriteFile(this, it, this.vfs)
            } else {
                DiskBasedVirtualDirectory(this, it, this.vfs)
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
        return DiskBasedVirtualReadWriteFile(this, File(file, name), this.vfs)
    }

    override fun directory(name: String): VirtualDirectory {
        return DiskBasedVirtualDirectory(this, File(this.file, name), this.vfs)
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

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DiskBasedVirtualDirectory

        return file == other.file
    }

    override fun hashCode(): Int {
        return file.hashCode()
    }

}