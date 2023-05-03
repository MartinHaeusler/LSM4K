package org.chronos.chronostore.lsm.merge.strategy

import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.lsm.event.InMemoryLsmInsertEvent
import org.chronos.chronostore.lsm.event.LsmCursorClosedEvent

interface MergeService {

    fun initialize(storeManager: StoreManager)

    /**
     * Performs the merge of store files now.
     *
     * This is a blocking operation. The operation may be deferred if a compaction is currently ongoing.
     *
     * @param major Use `true` for a major compaction (all files per store will be reduced to one), or `false`
     *              for a minor compaction ("best" candidate files will be merged into one, as decided by the
     *              merge strategy).
     */
    fun mergeNow(major: Boolean)

    fun handleInMemoryInsertEvent(event: InMemoryLsmInsertEvent)

    fun handleCursorClosedEvent(lsmCursorClosedEvent: LsmCursorClosedEvent)

}