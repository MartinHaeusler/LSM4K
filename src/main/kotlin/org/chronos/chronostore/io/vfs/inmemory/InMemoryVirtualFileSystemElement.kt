package org.chronos.chronostore.io.vfs.inmemory

import org.chronos.chronostore.io.vfs.VirtualFileSystemElement

interface InMemoryVirtualFileSystemElement: VirtualFileSystemElement {

    val fileSystem: InMemoryVirtualFileSystem

}