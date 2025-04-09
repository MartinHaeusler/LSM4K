package org.chronos.chronostore.lsm.filesplitter

import org.chronos.chronostore.util.bytes.Bytes

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