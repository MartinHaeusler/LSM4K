package org.chronos.chronostore.io.vfs

interface VirtualFile: VirtualFileSystemElement, InputSource {

    val length: Long

}