package org.chronos.chronostore.io.vfs

import org.chronos.chronostore.util.ListExtensions.headTail

interface VirtualFileSystem {

    val rootPath: String

    fun directory(name: String): VirtualDirectory

    fun directory(path: List<String>): VirtualDirectory {
        val (rootDirName, subPath) = path.headTail()
        val rootDir = this.directory(rootDirName)
        return subPath.fold(rootDir, VirtualDirectory::directory)
    }

    fun file(name: String): VirtualReadWriteFile

    fun listRootLevelElements(): List<VirtualFileSystemElement>

    fun mkdirs(path: List<String>): VirtualDirectory {
        require(path.isNotEmpty()) { "Cannot create directory for empty path!" }
        val first = path.first()
        val root = this.directory(first)
        val finalDirectory = path.asSequence().drop(1).fold(root) { acc, pathSegment -> acc.directory(pathSegment) }
        finalDirectory.mkdirs()
        return finalDirectory
    }

}