package org.chronos.chronostore.test.util.fakestoredsl

import org.chronos.chronostore.io.format.FileMetaData
import org.chronos.chronostore.lsm.merge.model.CompactableFile
import org.chronos.chronostore.util.FileIndex

class FakeCompactableFile(
    override val index: FileIndex,
    override val metadata: FileMetaData,
) : CompactableFile