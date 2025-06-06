package org.lsm4k.io.vfs.inmemory

import org.lsm4k.io.vfs.VirtualFileSystemElement

interface InMemoryVirtualFileSystemElement: VirtualFileSystemElement {

    val fileSystem: InMemoryVirtualFileSystem

}