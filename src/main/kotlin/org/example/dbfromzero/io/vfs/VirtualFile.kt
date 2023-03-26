package org.example.dbfromzero.io.vfs

interface VirtualFile: VirtualFileSystemElement, InputSource {

    val length: Long

}