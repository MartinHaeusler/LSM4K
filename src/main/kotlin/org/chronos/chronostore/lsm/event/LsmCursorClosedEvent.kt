package org.chronos.chronostore.lsm.event

import org.chronos.chronostore.lsm.LSMTree

class LsmCursorClosedEvent(
    val lsmTree: LSMTree
)