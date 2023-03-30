package org.chronos.chronostore.io.vfs

interface VirtualFileSystemElement {

    val name: String

    val parent: VirtualDirectory?

    val path: String

    fun exists(): Boolean


}