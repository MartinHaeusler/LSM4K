package org.chronos.chronostore.io.vfs.disk

import org.chronos.chronostore.io.vfs.VirtualFile
import java.io.File
import java.io.FileInputStream
import java.io.InputStream

open class DiskBasedVirtualFile(
    override val parent: DiskBasedVirtualDirectory?,
    protected val file: File,
) : VirtualFile {


    override fun exists(): Boolean {
        return this.file.exists() && this.file.isFile
    }

    override val length: Long
        get() = this.file.length()

    override val name: String
        get() = this.file.name

    override val path: String
        get() = this.file.absolutePath

    override fun createInputStream(): InputStream {
        return FileInputStream(file)
    }

    override fun toString(): String {
        return "File[${this.file.absolutePath}]"
    }


}