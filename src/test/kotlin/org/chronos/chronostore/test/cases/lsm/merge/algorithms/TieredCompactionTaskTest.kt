package org.chronos.chronostore.test.cases.lsm.merge.algorithms

import org.chronos.chronostore.api.compaction.TieredCompactionStrategy
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.lsm.merge.algorithms.CompactionTrigger
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.test.util.CompactionTestUtils.executeTieredCompactionSynchronously
import org.chronos.chronostore.test.util.VFSMode
import org.chronos.chronostore.test.util.VirtualFileSystemTest
import org.chronos.chronostore.test.util.fakestoredsl.FakeStoreDSL.Companion.createFakeTieredStore
import org.chronos.chronostore.util.unit.BinarySize.Companion.MiB
import strikt.api.expectThat
import strikt.assertions.*

class TieredCompactionTaskTest {

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
    fun spaceAmplificationTriggerWorks(mode: VFSMode) {
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
                )
            )

            // inspect the merges which have been executed by the compaction
            expectThat(store.executedMerges).single().and {
                get { this.keepTombstones }.isFalse() // no files in higher tiers
                get { this.fileIndices }.containsExactly(0, 1, 2)
                get { this.trigger }.isEqualTo(CompactionTrigger.SPACE_AMPLIFICATION)
            }
        }
    }

    @VirtualFileSystemTest
    fun sizeRatioTriggerWorks1(mode: VFSMode) {
        mode.withVFS { vfs ->
            // set up a fake store with some files
            val manifestFile = vfs.createManifestFile()
            val store = vfs.createFakeTieredStore {
                tier(0) {
                    file {
                        sizeOnDisk = 300.MiB
                    }
                }
                tier(1) {
                    file {
                        sizeOnDisk = 100.MiB
                    }
                }
                tier(2) {
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
                    minMergeTiers = 1,
                    // basically disable space amplification trigger
                    maxSpaceAmplificationPercent = 10.0
                )
            )

            // inspect the merges which have been executed by the compaction
            expectThat(store.executedMerges).single().and {
                get { this.keepTombstones }.isFalse() // no files in higher tiers
                get { this.fileIndices }.containsExactly(0, 1, 2)
                get { this.trigger }.isEqualTo(CompactionTrigger.SIZE_RATIO)
            }
        }
    }

    @VirtualFileSystemTest
    fun sizeRatioTriggerWorks2(mode: VFSMode) {
        mode.withVFS { vfs ->
            // set up a fake store with some files
            val manifestFile = vfs.createManifestFile()
            val store = vfs.createFakeTieredStore {
                tier(0) {
                    file(0) {
                        sizeOnDisk = 100.MiB
                    }
                }
                tier(1) {
                    file(1) {
                        sizeOnDisk = 200.MiB
                    }
                }
                tier(2) {
                    file(2) {
                        sizeOnDisk = 400.MiB
                    }
                }
            }

            // run the compaction
            store.executeTieredCompactionSynchronously(
                manifestFile = manifestFile,
                strategy = TieredCompactionStrategy(
                    numberOfTiers = 1,
                    minMergeTiers = 1,
                    // basically disable space amplification trigger
                    maxSpaceAmplificationPercent = 10.0
                )
            )

            // inspect the merges which have been executed by the compaction
            expectThat(store.executedMerges).single().and {
                get { this.keepTombstones }.isTrue() // we have files in higher tiers
                get { this.fileIndices }.containsExactly(0,1)
                get { this.trigger }.isEqualTo(CompactionTrigger.SIZE_RATIO)
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