package io.github.martinhaeusler.lsm4k.io.vfs.inmemory

import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFileSystemElement

interface InMemoryVirtualFileSystemElement : VirtualFileSystemElement {

    val fileSystem: InMemoryVirtualFileSystem

}