package org.chronos.chronostore.lsm

import org.chronos.chronostore.lsm.event.InMemoryLsmInsertEvent
import org.chronos.chronostore.lsm.event.LsmCursorClosedEvent

interface MergeStrategy {

    fun handleInMemoryInsertEvent(event: InMemoryLsmInsertEvent)

    fun handleCursorClosedEvent(lsmCursorClosedEvent: LsmCursorClosedEvent)

}