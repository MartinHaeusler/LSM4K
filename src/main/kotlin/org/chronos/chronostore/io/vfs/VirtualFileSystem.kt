package org.chronos.chronostore.io.vfs

interface VirtualFileSystem {

    val rootPath: String

    fun directory(name: String): VirtualDirectory

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