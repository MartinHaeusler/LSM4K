package io.github.martinhaeusler.lsm4k.test.cases.lsm.merge.algorithms

import io.github.martinhaeusler.lsm4k.api.compaction.TieredCompactionStrategy
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFileSystem
import io.github.martinhaeusler.lsm4k.lsm.compaction.algorithms.CompactionTrigger
import io.github.martinhaeusler.lsm4k.manifest.ManifestFile
import io.github.martinhaeusler.lsm4k.test.util.CompactionTestUtils.executeTieredCompactionSynchronously
import io.github.martinhaeusler.lsm4k.test.util.VFSMode
import io.github.martinhaeusler.lsm4k.test.util.VirtualFileSystemTest
import io.github.martinhaeusler.lsm4k.test.util.fakestoredsl.FakeStoreDSL.Companion.createFakeTieredStore
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.MiB
import strikt.api.expectThat
import strikt.assertions.*

class TieredCompactionProcessTest {

    @VirtualFileSystemTest
    fun tieredCompactionDoesNothingOnEmptyStore(mode: VFSMode) {
        mode.withVFS { vfs ->
            // set up a fake store with some files
            val store = vfs.createFakeTieredStore {
                tier(0)
            }

            // run the compaction
            store.executeTieredCompactionSynchronously()

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
                strategy = TieredCompactionStrategy(
                    numberOfTiers = 1,
                )
            )

            // inspect the merges which have been executed by the compaction
            expectThat(store.executedMerges).single().and {
                get { this.keepTombstones }.isFalse() // no files in higher tiers
                get { this.fileIndices }.containsExactlyInAnyOrder(0, 1, 2)
                get { this.trigger }.isEqualTo(CompactionTrigger.TIER_SPACE_AMPLIFICATION)
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
                        sizeOnDisk = 100.MiB
                    }
                }
                tier(1) {
                    file {
                        sizeOnDisk = 100.MiB
                    }
                }
                tier(2) {
                    file {
                        sizeOnDisk = 300.MiB
                    }
                }
            }

            // run the compaction
            store.executeTieredCompactionSynchronously(
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
                get { this.fileIndices }.containsExactlyInAnyOrder(0, 1, 2)
                get { this.trigger }.isEqualTo(CompactionTrigger.TIER_SIZE_RATIO)
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
                strategy = TieredCompactionStrategy(
                    numberOfTiers = 1,
                    minMergeTiers = 1,
                    // basically disable space amplification trigger
                    maxSpaceAmplificationPercent = 10.0
                )
            )

            // inspect the merges which have been executed by the compaction
            expectThat(store.executedMerges).single().and {
                get { this.keepTombstones }.isFalse() // size ratio trigger always starts with the oldest data
                get { this.fileIndices }.containsExactlyInAnyOrder(0, 1)
                get { this.trigger }.isEqualTo(CompactionTrigger.TIER_SIZE_RATIO)
            }
        }
    }

    @VirtualFileSystemTest
    fun heightReductionTriggerWorks(mode: VFSMode) {
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
                        sizeOnDisk = 100.MiB
                    }
                }
                tier(2) {
                    file(2) {
                        sizeOnDisk = 100.MiB
                    }
                }
                tier(3) {
                    file(3) {
                        sizeOnDisk = 100.MiB
                    }
                }
            }

            // run the compaction
            store.executeTieredCompactionSynchronously(
                strategy = TieredCompactionStrategy(
                    numberOfTiers = 3,
                    minMergeTiers = 1,
                    // disable size ratio trigger
                    sizeRatio = 10.0,
                    // disable space amplification trigger
                    maxSpaceAmplificationPercent = 10.0
                )
            )

            // inspect the merges which have been executed by the compaction
            expectThat(store.executedMerges).single().and {
                get { this.keepTombstones }.isFalse() // tree height trigger always starts with the oldest data
                get { this.fileIndices }.containsExactlyInAnyOrder(0, 1, 2)
                get { this.trigger }.isEqualTo(CompactionTrigger.TIER_HEIGHT_REDUCTION)
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