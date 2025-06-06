package org.lsm4k.lsm.compaction.model

import org.lsm4k.io.format.FileMetaData
import org.lsm4k.lsm.LSMTreeFile
import org.lsm4k.util.FileIndex
import org.lsm4k.util.unit.BinarySize
import org.lsm4k.util.unit.SizeUnit

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