package org.example.dbfromzero.io.vfs.disk

import org.example.dbfromzero.io.vfs.VirtualDirectory
import org.example.dbfromzero.io.vfs.VirtualFile
import org.example.dbfromzero.io.vfs.VirtualReadWriteFile
import java.io.File

class DiskBasedVirtualDirectory(
    private val file: File
) : VirtualDirectory {


    override fun list(): List<String> {
        return this.file.list()?.asList() ?: emptyList()
    }

    override fun mkdirs() {
        this.file.mkdirs()
    }

    override fun clear() {
        for (file in this.file.listFiles() ?: emptyArray()) {
            file.deleteRecursively()
        }
    }

    override fun file(name: String): VirtualReadWriteFile {
        return DiskBasedVirtualReadWriteFile(File(file, name), "-tmp")
    }

    override fun directory(name: String): VirtualDirectory {
        return DiskBasedVirtualDirectory(File(this.file, name))
    }

    override val name: String
        get() = this.file.name

    override val parent: VirtualDirectory? by lazy {
        this.file.parentFile?.let { DiskBasedVirtualDirectory(it) }
    }

    override val path: String
        get() = this.file.absolutePath

    override fun exists(): Boolean {
        return this.file.exists() && this.file.isDirectory
    }

}