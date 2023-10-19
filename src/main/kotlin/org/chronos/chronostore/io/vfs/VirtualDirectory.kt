package org.chronos.chronostore.io.vfs

interface VirtualDirectory: VirtualFileSystemElement {

    fun list(): List<String>

    fun listElements(): List<VirtualFileSystemElement>

    fun listFiles(): List<VirtualFile> {
        return this.listElements().filterIsInstance<VirtualFile>()
    }

    fun listDirectories(): List<VirtualDirectory> {
        return this.listElements().filterIsInstance<VirtualDirectory>()
    }

    fun mkdirs()

    fun clear()

    fun file(name: String): VirtualReadWriteFile

    fun directory(name: String): VirtualDirectory

    fun delete()

}