package io.github.martinhaeusler.lsm4k.lsm.compaction.model

import io.github.martinhaeusler.lsm4k.io.format.FileMetaData
import io.github.martinhaeusler.lsm4k.lsm.LSMTreeFile
import io.github.martinhaeusler.lsm4k.util.FileIndex
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize
import io.github.martinhaeusler.lsm4k.util.unit.SizeUnit

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