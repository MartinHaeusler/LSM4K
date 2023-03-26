package org.example.dbfromzero.io.vfs

interface VirtualDirectory: VirtualFileSystemElement {

    fun list(): List<String>

    fun mkdirs()

    fun clear()

    fun file(name: String): VirtualReadWriteFile

    fun directory(name: String): VirtualDirectory

}