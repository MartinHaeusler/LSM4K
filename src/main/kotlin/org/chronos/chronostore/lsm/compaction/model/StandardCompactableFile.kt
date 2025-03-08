package org.chronos.chronostore.lsm.compaction.model

import org.chronos.chronostore.io.format.FileMetaData
import org.chronos.chronostore.lsm.LSMTreeFile
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.unit.BinarySize
import org.chronos.chronostore.util.unit.SizeUnit

class StandardCompactableFile(
    val lsmFile: LSMTreeFile,
) : CompactableFile {

    override val index: FileIndex
        get() = this.lsmFile.index

    override val metadata: FileMetaData
        get() = this.lsmFile.header.metaData

    override val sizeOnDisk: BinarySize
        get() = BinarySize(this.lsmFile.virtualFile.length, SizeUnit.BYTE)

}