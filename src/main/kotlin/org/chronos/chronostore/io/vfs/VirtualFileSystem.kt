package org.chronos.chronostore.io.vfs

interface VirtualFileSystem {

    fun directory(name: String): VirtualDirectory

    fun file(name: String): VirtualReadWriteFile

}