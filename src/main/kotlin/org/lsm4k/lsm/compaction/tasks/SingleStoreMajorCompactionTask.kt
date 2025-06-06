package org.lsm4k.lsm.compaction.tasks

import org.lsm4k.async.taskmonitor.TaskMonitor
import org.lsm4k.async.tasks.AsyncTask
import org.lsm4k.impl.Killswitch
import org.lsm4k.lsm.LSMTree
import org.lsm4k.lsm.compaction.algorithms.FullCompactionTask
import org.lsm4k.lsm.compaction.model.StandardCompactableStore
import org.lsm4k.manifest.ManifestFile

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