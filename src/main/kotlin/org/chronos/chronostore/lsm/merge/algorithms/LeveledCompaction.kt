package org.chronos.chronostore.lsm.merge.algorithms

import org.chronos.chronostore.api.compaction.LeveledCompactionStrategy
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.manifest.StoreMetadata

class LeveledCompaction(
    val manifestFile: ManifestFile,
    val store: StoreImpl,
    val storeMetadata: StoreMetadata,
    val configuration: LeveledCompactionStrategy,
    val monitor: TaskMonitor,
) {

    fun runCompaction(){

    }

}