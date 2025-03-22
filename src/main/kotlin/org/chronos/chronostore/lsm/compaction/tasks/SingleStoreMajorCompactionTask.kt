package org.chronos.chronostore.lsm.compaction.tasks

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.impl.Killswitch
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.lsm.compaction.algorithms.FullCompactionTask
import org.chronos.chronostore.lsm.compaction.model.StandardCompactableStore
import org.chronos.chronostore.manifest.ManifestFile

class SingleStoreMajorCompactionTask(
    private val lsmTree: LSMTree,
    private val manifestFile: ManifestFile,
    private val killswitch: Killswitch,
) : AsyncTask {

    override val name: String
        get() = "Major Compaction of '${lsmTree.storeId}'"

    override fun run(monitor: TaskMonitor) {
        try {
            val compactableStore = StandardCompactableStore(this.lsmTree)
            val compaction = FullCompactionTask(
                manifestFile = this.manifestFile,
                store = compactableStore,
            )
            compaction.runCompaction(monitor)
        } catch (t: Throwable) {
            this.killswitch.panic("An unexpected error occurred during Major Compaction of store '${lsmTree.storeId}': ${t}", t)
            throw t
        }
    }

}