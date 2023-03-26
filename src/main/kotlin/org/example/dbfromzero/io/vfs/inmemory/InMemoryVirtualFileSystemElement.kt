package org.example.dbfromzero.io.vfs.inmemory

import org.example.dbfromzero.io.vfs.VirtualFileSystemElement

interface InMemoryVirtualFileSystemElement: VirtualFileSystemElement {

    val fileSystem: InMemoryFileSystem

}