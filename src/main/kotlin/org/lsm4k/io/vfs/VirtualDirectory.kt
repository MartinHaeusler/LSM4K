package org.lsm4k.io.vfs

import org.lsm4k.util.ListExtensions.headTail

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

    fun directory(path: List<String>): VirtualDirectory {
        val (rootDirName, subPath) = path.headTail()
        val rootDir = this.directory(rootDirName)
        return subPath.fold(rootDir, VirtualDirectory::directory)
    }

}