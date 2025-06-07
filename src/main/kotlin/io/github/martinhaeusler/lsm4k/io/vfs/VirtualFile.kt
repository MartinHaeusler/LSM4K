package io.github.martinhaeusler.lsm4k.io.vfs

interface VirtualFile : VirtualFileSystemElement, InputSource {

    /** Returns the length of the file in Bytes. Will return zero for directories and non-existing files. */
    val length: Long

}