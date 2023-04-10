package org.chronos.chronostore.lsm.event

import org.chronos.chronostore.lsm.LSMTree

class InMemoryLsmInsertEvent(
    val lsmTree: LSMTree,
    val insertedCount: Int,
    val currentInMemoryElementCount: Int,
)