package org.chronos.chronostore.lsm.event

import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.util.unit.BinarySize

class InMemoryLsmInsertEvent(
    val lsmTree: LSMTree,
    val inMemoryElementCountBefore: Int,
    val inMemoryElementCountAfter: Int,
    val inMemorySizeBefore: BinarySize,
    val inMemorySizeAfter: BinarySize,
)