package io.github.martinhaeusler.lsm4k.test.util.fakestoredsl

import io.github.martinhaeusler.lsm4k.io.format.FileMetaData
import io.github.martinhaeusler.lsm4k.lsm.compaction.model.CompactableFile
import io.github.martinhaeusler.lsm4k.util.FileIndex
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize

class FakeCompactableFile(
    override val index: FileIndex,
    override val metadata: FileMetaData,
    override val sizeOnDisk: BinarySize,
) : CompactableFile