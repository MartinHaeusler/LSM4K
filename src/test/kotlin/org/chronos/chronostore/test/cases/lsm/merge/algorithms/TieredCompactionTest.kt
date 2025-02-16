package org.chronos.chronostore.test.cases.lsm.merge.algorithms

import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.test.util.CompactionTestUtils.executeTieredCompactionSynchronously
import org.chronos.chronostore.test.util.VFSMode
import org.chronos.chronostore.test.util.VirtualFileSystemTest
import org.chronos.chronostore.test.util.fakestoredsl.FakeStoreDSL.Companion.createFakeTieredStore
import strikt.api.expectThat
import strikt.assertions.isEmpty

class TieredCompactionTest {

    @VirtualFileSystemTest
    fun tieredCompactionDoesNothingOnEmptyStore(mode: VFSMode) {
        mode.withVFS { vfs ->
            // set up a fake store with some files
            val manifestFile = vfs.createManifestFile()
            val store = vfs.createFakeTieredStore {
                tier(0)
            }

            // run the compaction
            store.executeTieredCompactionSynchronously(manifestFile)

            // inspect the merges which have been executed by the compaction
            expectThat(store.executedMerges).isEmpty()
        }
    }

    // =================================================================================================================
    // HELPER FUNCTIONS
    // =================================================================================================================

    private fun VirtualFileSystem.createManifestFile(): ManifestFile {
        return ManifestFile(file(ManifestFile.FILE_NAME))
    }


}