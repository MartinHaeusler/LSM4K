package org.lsm4k.lsm.compaction.model

import org.lsm4k.io.format.FileMetaData
import org.lsm4k.util.FileIndex
import org.lsm4k.util.Timestamp
import org.lsm4k.util.unit.BinarySize

interface CompactableFile {

    val index: FileIndex

    val metadata: FileMetaData

    val sizeOnDisk: BinarySize

    fun age(now: Timestamp): Long {
        val createdAt = this.metadata.createdAt
        return (now - createdAt).coerceAtLeast(0)
    }

}