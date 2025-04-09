package org.chronos.chronostore.lsm.filesplitter

import org.chronos.chronostore.util.bytes.Bytes

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