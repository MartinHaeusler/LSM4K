package io.github.martinhaeusler.lsm4k.io.vfs

interface VirtualFileSystemElement {

    val name: String

    val parent: VirtualDirectory?

    val path: String

    fun exists(): Boolean


}