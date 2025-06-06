package org.lsm4k.lsm.filesplitter

import org.lsm4k.util.bytes.Bytes

data object SingleFileSplitter : FileSplitter {

    override fun splitHere(
        fileSizeInBytes: Long,
        firstKeyInFile: Bytes?,
        lastKeyInFile: Bytes?,
    ): Boolean {
        // we want a single file -> we never split.
        return false
    }


}