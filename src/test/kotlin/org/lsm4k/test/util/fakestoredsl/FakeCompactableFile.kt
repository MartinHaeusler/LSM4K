package org.lsm4k.test.util.fakestoredsl

import org.lsm4k.io.format.FileMetaData
import org.lsm4k.lsm.compaction.model.CompactableFile
import org.lsm4k.util.FileIndex
import org.lsm4k.util.unit.BinarySize

class FakeCompactableFile(
    override val index: FileIndex,
    override val metadata: FileMetaData,
    override val sizeOnDisk: BinarySize,
) : CompactableFile