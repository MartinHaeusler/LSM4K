package org.chronos.chronostore.lsm.merge.strategy

import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.lsm.event.LsmCursorClosedEvent
import org.chronos.chronostore.wal.WriteAheadLog

interface MergeService {

    fun initialize(storeManager: StoreManager, writeAheadLog: WriteAheadLog)

    /**
     * Performs a major compaction of store files.
     *
     * This is a blocking operation. The operation may be deferred if a compaction is currently ongoing.
     *
     * @param taskMonitor The task monitor to use. Defaults to a new monitor.
     */
    fun performMajorCompaction(taskMonitor: TaskMonitor = TaskMonitor.create())

    /**
     * Performs a minor compaction of store files.
     *
     * This is a blocking operation. The operation may be deferred if a compaction is currently ongoing.
     *
     * @param taskMonitor The task monitor to use. Defaults to a new monitor.
     */
    fun performMinorCompaction(taskMonitor: TaskMonitor = TaskMonitor.create())

    fun flushAllInMemoryStoresToDisk(taskMonitor: TaskMonitor = TaskMonitor.create())

}