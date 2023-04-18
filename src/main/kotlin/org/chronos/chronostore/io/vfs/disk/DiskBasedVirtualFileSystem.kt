package org.chronos.chronostore.io.vfs.disk

import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile
import java.io.File

class DiskBasedVirtualFileSystem(
    val rootDir: File
) : VirtualFileSystem {

    init {
        require(this.rootDir.exists()) { "Argument 'rootDir' refers to a non-existing location: ${rootDir.absolutePath}" }
        require(this.rootDir.isDirectory) { "Argument 'rootDir' refers to an element which is not a directory: ${rootDir.absolutePath}" }
    }

    override fun directory(name: String): VirtualDirectory {
        return DiskBasedVirtualDirectory(parent = null, File(this.rootDir, name))
    }

    override fun file(name: String): VirtualReadWriteFile {
        return DiskBasedVirtualReadWriteFile(parent = null, File(this.rootDir, name))
    }

    override fun toString(): String {
        return this.rootDir.absolutePath
    }

}