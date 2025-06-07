package io.github.martinhaeusler.lsm4k.lsm.filesplitter

import io.github.martinhaeusler.lsm4k.util.bytes.Bytes

data class SizeBasedFileSplitter(
    private val targetFileSizeInBytes: Long,
) : FileSplitter {

    override fun splitHere(
        fileSizeInBytes: Long,
        firstKeyInFile: Bytes?,
        lastKeyInFile: Bytes?,
    ): Boolean {
        return fileSizeInBytes >= this.targetFileSizeInBytes
    }

}