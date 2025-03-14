package org.chronos.chronostore.lsm.compaction.tasks

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.lsm.compaction.algorithms.FullCompactionTask
import org.chronos.chronostore.lsm.compaction.model.StandardCompactableStore
import org.chronos.chronostore.manifest.ManifestFile

class SingleStoreMajorCompactionTask(
    val lsmTree: LSMTree,
    val manifestFile: ManifestFile,
): AsyncTask {

    override val name: String
        get() = "Major Compaction of '${lsmTree.storeId}'"

    override fun run(monitor: TaskMonitor) {
        val compactableStore = StandardCompactableStore(this.lsmTree)
        val compaction = FullCompactionTask(
            manifestFile = this.manifestFile,
            store = compactableStore,
        )
        compaction.runCompaction(monitor)
    }

}