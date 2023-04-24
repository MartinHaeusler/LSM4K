package org.chronos.chronostore.lsm.merge.strategy

import org.chronos.chronostore.lsm.event.InMemoryLsmInsertEvent
import org.chronos.chronostore.lsm.event.LsmCursorClosedEvent

interface MergeService {

    fun initialize()

    fun handleInMemoryInsertEvent(event: InMemoryLsmInsertEvent)

    fun handleCursorClosedEvent(lsmCursorClosedEvent: LsmCursorClosedEvent)

}