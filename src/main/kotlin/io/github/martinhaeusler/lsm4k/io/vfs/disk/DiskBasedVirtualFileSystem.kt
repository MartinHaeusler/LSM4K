package io.github.martinhaeusler.lsm4k.io.vfs.disk

import io.github.martinhaeusler.lsm4k.io.vfs.VirtualDirectory
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFileSystem
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFileSystemElement
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualReadWriteFile
import java.io.File

class DiskBasedVirtualFileSystem(
    val rootDir: File,
    val settings: DiskBasedVirtualFileSystemSettings,
) : VirtualFileSystem {

    init {
        require(this.rootDir.exists()) { "Argument 'rootDir' refers to a non-existing location: ${rootDir.absolutePath}" }
        require(this.rootDir.isDirectory) { "Argument 'rootDir' refers to an element which is not a directory: ${rootDir.absolutePath}" }
    }

    override val rootPath: String
        get() = this.rootDir.canonicalPath

    override fun listRootLevelElements(): List<VirtualFileSystemElement> {
        val files = this.rootDir.listFiles() ?: emptyArray()
        return files.map {
            if (it.isFile) {
                DiskBasedVirtualReadWriteFile(
                    parent = null,
                    file = it,
                    vfs = this
                )
            } else {
                DiskBasedVirtualDirectory(
                    parent = null,
                    file = it,
                    vfs = this
                )
            }
        }
    }

    override fun directory(name: String): VirtualDirectory {
        return DiskBasedVirtualDirectory(
            parent = null,
            file = File(this.rootDir, name),
            vfs = this
        )
    }

    override fun file(name: String): VirtualReadWriteFile {
        return DiskBasedVirtualReadWriteFile(
            parent = null,
            file = File(this.rootDir, name),
            vfs = this
        )
    }

    override fun toString(): String {
        return this.rootDir.absolutePath
    }

}