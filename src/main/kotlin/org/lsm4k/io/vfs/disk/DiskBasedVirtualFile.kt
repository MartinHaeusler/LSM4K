package org.lsm4k.io.vfs.disk

import org.lsm4k.io.vfs.VirtualFile
import java.io.File
import java.io.FileInputStream
import java.io.InputStream

open class DiskBasedVirtualFile(
    override val parent: DiskBasedVirtualDirectory?,
    protected val file: File,
    protected val vfs: DiskBasedVirtualFileSystem,
) : VirtualFile {


    override fun exists(): Boolean {
        return this.file.exists() && this.file.isFile
    }

    val fileOnDisk: File
        get(){
            return this.file
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

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DiskBasedVirtualFile

        return file == other.file
    }

    override fun hashCode(): Int {
        return file.hashCode()
    }


}