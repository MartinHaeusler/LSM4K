package org.chronos.chronostore.lsm.event

import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.util.unit.BinarySize

class InMemoryLsmFlushEvent(
    val lsmTree: LSMTree,
    val flushedEntryCount: Int,
)