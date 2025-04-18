package org.chronos.chronostore.test.cases.lsm.merge.algorithms

import org.chronos.chronostore.api.compaction.TieredCompactionStrategy
import org.chronos.chronostore.io.vfs.VirtualFileSystem
import org.chronos.chronostore.lsm.compaction.algorithms.CompactionTrigger
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.test.util.CompactionTestUtils.executeTieredCompactionSynchronously
import org.chronos.chronostore.test.util.VFSMode
import org.chronos.chronostore.test.util.VirtualFileSystemTest
import org.chronos.chronostore.test.util.fakestoredsl.FakeStoreDSL.Companion.createFakeTieredStore
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.unit.BinarySize.Companion.MiB
import org.junit.jupiter.params.ParameterizedTest
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

    @ParameterizedTest
    @VirtualFileSystemTest
    fun canMergeFilesUpwards(mode: VFSMode) {
        mode.withVFS { vfs ->
            // set up a fake store with some files
            vfs.createManifestFile()
            val store = vfs.createFakeTieredStore {
                tier(0) {
                    file(0) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("e")
                        maxKey = Bytes.of("t")
                    }
                    file(1) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("e")
                        maxKey = Bytes.of("t")
                    }
                    file(2) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("e")
                        maxKey = Bytes.of("t")
                    }
                    file(3) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("e")
                        maxKey = Bytes.of("t")
                    }
                    file(4) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("e")
                        maxKey = Bytes.of("t")
                    }
                    file(5) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("e")
                        maxKey = Bytes.of("t")
                    }
                    file(6) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("e")
                        maxKey = Bytes.of("t")
                    }
                    file(7) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("e")
                        maxKey = Bytes.of("t")
                    }
                }
            }

            // run the compaction
            store.executeTieredCompactionSynchronously(
                strategy = TieredCompactionStrategy(
                    numberOfTiers = 3,
                )
            )

            // inspect the merges which have been executed by the compaction
            expectThat(store.executedMerges).single().and {
                get { this.keepTombstones }.isFalse()
                get { this.fileIndices }.containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7)
                get { this.trigger }.isEqualTo(CompactionTrigger.TIER_TIER0)
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