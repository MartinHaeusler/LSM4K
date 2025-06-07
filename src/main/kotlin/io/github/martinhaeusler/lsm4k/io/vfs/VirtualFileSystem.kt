package io.github.martinhaeusler.lsm4k.io.vfs

import io.github.martinhaeusler.lsm4k.util.ListExtensions.headTail

interface VirtualFileSystem {

    val rootPath: String

    fun directory(name: String): VirtualDirectory

    fun directory(path: List<String>): VirtualDirectory {
        val (rootDirName, subPath) = path.headTail()
        val rootDir = this.directory(rootDirName)
        return subPath.fold(rootDir, VirtualDirectory::directory)
    }

    fun file(path: List<String>): VirtualReadWriteFile {
        val (rootDirName, subPath) = path.headTail()
        val rootDir = this.directory(rootDirName)
        val intermediatePath = subPath.dropLast(1)
        val parentDir = if (intermediatePath.isEmpty()) {
            rootDir
        } else {
            intermediatePath.fold(rootDir, VirtualDirectory::directory)
        }
        return parentDir.file(path.last())
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