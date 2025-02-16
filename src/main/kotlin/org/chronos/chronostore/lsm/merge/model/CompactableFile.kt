package org.chronos.chronostore.lsm.merge.model

import org.chronos.chronostore.io.format.FileMetaData
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.Timestamp

interface CompactableFile {

    val index: FileIndex

    val metadata: FileMetaData

    fun age(now: Timestamp): Long {
        val createdAt = this.metadata.createdAt
        return (now - createdAt).coerceAtLeast(0)
    }

}