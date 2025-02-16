package org.chronos.chronostore.lsm.merge.model

import org.chronos.chronostore.io.format.FileMetaData
import org.chronos.chronostore.lsm.LSMTreeFile
import org.chronos.chronostore.util.FileIndex

class StandardCompactableFile(
    val lsmFile: LSMTreeFile,
): CompactableFile {

    override val index: FileIndex
        get() = this.lsmFile.index

    override val metadata: FileMetaData
        get() = this.lsmFile.header.metaData

}