package org.chronos.chronostore.lsm.merge.strategy

import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.event.InMemoryLsmInsertEvent
import org.chronos.chronostore.lsm.event.LsmCursorClosedEvent
import org.chronos.chronostore.wal.WriteAheadLog

interface MergeService {

    fun initialize(storeManager: StoreManager, writeAheadLog: WriteAheadLog)

    /**
     * Performs the merge of store files now.
     *
     * This is a blocking operation. The operation may be deferred if a compaction is currently ongoing.
     *
     * @param major Use `true` for a major compaction (all files per store will be reduced to one), or `false`
     *              for a minor compaction ("best" candidate files will be merged into one, as decided by the
     *              merge strategy).
     *
     * @param taskMonitor The task monitor to use. Defaults to a new monitor.
     */
    fun mergeNow(major: Boolean, taskMonitor: TaskMonitor = TaskMonitor.create())

    fun flushAllInMemoryStoresToDisk(taskMonitor: TaskMonitor = TaskMonitor.create())

    fun handleInMemoryInsertEvent(event: InMemoryLsmInsertEvent)

    fun handleCursorClosedEvent(lsmCursorClosedEvent: LsmCursorClosedEvent)

}