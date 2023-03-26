package org.example.dbfromzero.io.vfs.disk

import org.example.dbfromzero.io.vfs.VirtualDirectory
import org.example.dbfromzero.io.vfs.VirtualFile
import java.io.File
import java.io.FileInputStream

import java.io.InputStream

open class DiskBasedVirtualFile(
    protected val file: File,
) : VirtualFile {


    override fun exists(): Boolean {
        return this.file.exists() && this.file.isFile
    }

    override val length: Long
        get() = this.file.length()

    override val name: String
        get() = this.file.name

    override val parent: VirtualDirectory by lazy {
        DiskBasedVirtualDirectory(this.file.parentFile)
    }

    override val path: String
        get() = this.file.absolutePath

    override fun createInputStream(): InputStream {
        return FileInputStream(file)
    }

    override fun toString(): String {
        return "File[${this.file.absolutePath}]"
    }


}