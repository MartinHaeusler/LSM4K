package org.chronos.chronostore.test.cases.lsm.merge.algorithms

import org.chronos.chronostore.api.compaction.TieredCompactionStrategy
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.test.util.CompactionTestUtils.executeTieredCompactionSynchronously
import org.chronos.chronostore.test.util.VFSMode
import org.chronos.chronostore.test.util.VirtualFileSystemTest
import org.chronos.chronostore.test.util.fakestoredsl.FakeStoreDSL.Companion.createFakeTieredStore
import org.chronos.chronostore.util.unit.BinarySize.Companion.MiB
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.isEmpty
import strikt.assertions.isFalse
import strikt.assertions.single

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

    @VirtualFileSystemTest
    fun tieredCompactionCanMergeTwoFiles(mode: VFSMode) {
        mode.withVFS { vfs ->
            // set up a fake store with some files
            val manifestFile = vfs.createManifestFile()
            val store = vfs.createFakeTieredStore {
                tier {
                    file {
                        sizeOnDisk = 100.MiB
                    }
                }
                tier {
                    file {
                        sizeOnDisk = 100.MiB
                    }
                }
            }

            // run the compaction
            store.executeTieredCompactionSynchronously(
                manifestFile = manifestFile,
                strategy = TieredCompactionStrategy(
                    numberOfTiers = 1,
                    maxSizeAmplificationPercent = 0.9
                )
            )

            // inspect the merges which have been executed by the compaction
            expectThat(store.executedMerges).single().and {
                get { this.keepTombstones }.isFalse() // no files in higher tiers
                get { this.fileIndices }.containsExactly(0, 1)
            }
        }
    }

    // =================================================================================================================
    // HELPER FUNCTIONS
    // =================================================================================================================

    private fun VirtualFileSystem.createManifestFile(): ManifestFile {
        return ManifestFile(file(ManifestFile.FILE_NAME))
    }


}