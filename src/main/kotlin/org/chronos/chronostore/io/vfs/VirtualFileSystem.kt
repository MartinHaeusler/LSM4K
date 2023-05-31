package org.chronos.chronostore.io.vfs

interface VirtualFileSystem {

    val rootPath: String

    fun directory(name: String): VirtualDirectory

    fun file(name: String): VirtualReadWriteFile

    fun listRootLevelElements(): List<VirtualFileSystemElement>

}