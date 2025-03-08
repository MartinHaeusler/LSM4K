package org.chronos.chronostore.test.util.fakestoredsl

import org.chronos.chronostore.io.format.FileMetaData
import org.chronos.chronostore.lsm.compaction.model.CompactableFile
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.unit.BinarySize

class FakeCompactableFile(
    override val index: FileIndex,
    override val metadata: FileMetaData,
    override val sizeOnDisk: BinarySize,
) : CompactableFile