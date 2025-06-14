package io.github.martinhaeusler.lsm4k.test.cases.lsm.merge.algorithms

import io.github.martinhaeusler.lsm4k.api.compaction.LeveledCompactionStrategy
import io.github.martinhaeusler.lsm4k.api.compaction.LeveledCompactionStrategy.FileSelectionStrategy.BY_MOST_DELETIONS
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFileSystem
import io.github.martinhaeusler.lsm4k.lsm.compaction.algorithms.CompactionTrigger
import io.github.martinhaeusler.lsm4k.lsm.compaction.algorithms.LeveledCompactionProcess
import io.github.martinhaeusler.lsm4k.manifest.ManifestFile
import io.github.martinhaeusler.lsm4k.test.util.CompactionTestUtils.executeLeveledCompactionSynchronously
import io.github.martinhaeusler.lsm4k.test.util.VFSMode
import io.github.martinhaeusler.lsm4k.test.util.VirtualFileSystemTest
import io.github.martinhaeusler.lsm4k.test.util.fakestoredsl.FakeStoreDSL.Companion.createFakeLeveledStore
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.GiB
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.MiB
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import strikt.api.expectThat
import strikt.assertions.*

class LeveledCompactionProcessTest {

    @Test
    fun canComputeTargetLevelSizes1() {
        expectThat(
            LeveledCompactionProcess.computeTargetLevelSizes(
                longArrayOf(
                    /* L0 */ 0,
                    /* L1 */ 0,
                    /* L2 */ 0,
                    /* L3 */ 0,
                    /* L4 */ 0,
                    /* L5 */ 300.MiB.bytes,
                ),
                maxLevels = 6,
                baseLevelMinSize = 100.MiB,
                levelSizeMultiplier = 10.0,
            )
        ).contentEquals(
            longArrayOf(
                /* L0 */ 0,
                /* L1 */ 0,
                /* L2 */ 0,
                /* L3 */ 0,
                /* L4 */ 30.MiB.bytes,
                /* L5 */ 300.MiB.bytes,
            )
        )
    }

    @Test
    fun canComputeTargetLevelSizes2() {
        expectThat(
            LeveledCompactionProcess.computeTargetLevelSizes(
                longArrayOf(
                    /* L0 */ 0,
                    /* L1 */ 300.MiB.bytes,
                ),
                maxLevels = 6,
                baseLevelMinSize = 100.MiB,
                levelSizeMultiplier = 10.0,
            )
        ).contentEquals(
            longArrayOf(
                /* L0 */ 0,
                /* L1 */ 0,
                /* L2 */ 0,
                /* L3 */ 0,
                /* L4 */ 30.MiB.bytes,
                /* L5 */ 300.MiB.bytes,
            )
        )
    }

    @Test
    fun canComputeTargetLevelSizes3() {
        expectThat(
            LeveledCompactionProcess.computeTargetLevelSizes(
                longArrayOf(
                    /* L0 */ 0,
                    /* L1 */ 0,
                    /* L2 */ 0,
                    /* L3 */ 0,
                    /* L4 */ 0,
                    /* L5 */ 30.GiB.bytes,
                ),
                maxLevels = 6,
                baseLevelMinSize = 100.MiB,
                levelSizeMultiplier = 10.0,
            )
        ).contentEquals(
            longArrayOf(
                /* L0 */ 0,
                /* L1 */ 0,
                /* L2 */ 3.GiB.bytes / 100,
                /* L3 */ 3.GiB.bytes / 10,
                /* L4 */ 3.GiB.bytes,
                /* L5 */ 30.GiB.bytes,
            )
        )
    }

    @Test
    fun canComputeCurrentSizeToTargetSizeRatios() {
        expectThat(
            LeveledCompactionProcess.computeCurrentSizeToTargetSizeRatios(
                currentSizes = longArrayOf(
                    /* L0 */ 10.GiB.bytes,
                    /* L1 */ 1.GiB.bytes,
                    /* L2 */ 5.GiB.bytes / 100,
                    /* L3 */ 8.GiB.bytes / 10,
                    /* L4 */ 3.GiB.bytes,
                    /* L5 */ 30.GiB.bytes,
                ),
                targetSizes = longArrayOf(
                    /* L0 */ 0,
                    /* L1 */ 0,
                    /* L2 */ 3.GiB.bytes / 100,
                    /* L3 */ 3.GiB.bytes / 10,
                    /* L4 */ 3.GiB.bytes,
                    /* L5 */ 30.GiB.bytes,
                )
            )
        ).get { roundTo2Decimals() }
            .contentEquals(
                doubleArrayOf(
                    /* L0 */ 0.0, // no target
                    /* L1 */ 0.0, // no target
                    /* L2 */ 1.66,
                    /* L3 */ 2.66,
                    /* L4 */ 0.0, // ratio <= 1.0
                    /* L5 */ 0.0, // ratio <= 1.0
                )
            )
    }


    @Test
    fun canSelectLevelWithHighestSizeToTargetRatio() {
        expectThat(
            LeveledCompactionProcess.selectLevelWithHighestSizeToTargetRatio(
                doubleArrayOf(
                    /* L0 */ 0.0,
                    /* L1 */ 0.0,
                    /* L2 */ 1.66,
                    /* L3 */ 2.66,
                    /* L4 */ 0.0,
                    /* L5 */ 0.0,
                )
            )
        ).isEqualTo(3) // L3 has the highest ratio and thus the highest compaction priority.
    }

    @ParameterizedTest
    @VirtualFileSystemTest
    fun level0FlushTriggerWorks(mode: VFSMode) {
        mode.withVFS { vfs ->
            // set up a fake store with some files
            val manifestFile = vfs.createManifestFile()
            val store = vfs.createFakeLeveledStore {
                level(0) {
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
                }
                level(1) {
                    file(3) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("a")
                        maxKey = Bytes.of("c")
                    }
                    file(4) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("d")
                        maxKey = Bytes.of("g")
                    }
                    file(5) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("h")
                        maxKey = Bytes.of("r")
                    }
                    file(6) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("s")
                        maxKey = Bytes.of("z")
                    }
                }
                level(2) {
                    file(7) {
                        sizeOnDisk = 300.MiB
                        minKey = Bytes.of("a")
                        maxKey = Bytes.of("z")
                    }
                }
            }

            // run the compaction
            store.executeLeveledCompactionSynchronously(
                strategy = LeveledCompactionStrategy(
                    maxLevels = 3,
                    level0FileNumberCompactionTrigger = 2,
                )
            )

            // inspect the merges which have been executed by the compaction
            expectThat(store.executedMerges).single().and {
                get { this.keepTombstones }.isTrue()
                get { this.fileIndices }.containsExactlyInAnyOrder(0, 1, 2, 4, 5, 6)
                get { this.trigger }.isEqualTo(CompactionTrigger.LEVELED_LEVEL0)
            }
        }
    }

    @ParameterizedTest
    @VirtualFileSystemTest
    fun leveledTargetSizeRatioTriggerWorks(mode: VFSMode) {
        mode.withVFS { vfs ->
            // set up a fake store with some files
            val manifestFile = vfs.createManifestFile()
            val store = vfs.createFakeLeveledStore {
                level(0) {
                    file(0) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("e")
                        maxKey = Bytes.of("t")
                    }
                }
                level(1) {
                    file(3) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("a")
                        maxKey = Bytes.of("c")
                        headEntries = 900
                        totalEntries = 1000
                        // HHR = 9/10
                    }
                    file(4) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("d")
                        maxKey = Bytes.of("g")
                        headEntries = 200
                        totalEntries = 1000
                        // HHR = 2/10
                    }
                    file(5) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("h")
                        maxKey = Bytes.of("r")
                        headEntries = 1000
                        totalEntries = 1000
                        // HHR = 1
                    }
                    file(6) {
                        sizeOnDisk = 100.MiB
                        minKey = Bytes.of("s")
                        maxKey = Bytes.of("z")
                        headEntries = 1000
                        totalEntries = 1500
                        // HHR = 10/15 = 2/3
                    }
                }
                level(2) {
                    file(7) {
                        sizeOnDisk = 300.MiB
                        minKey = Bytes.of("a")
                        maxKey = Bytes.of("z")
                    }
                }
            }

            // run the compaction
            store.executeLeveledCompactionSynchronously(
                strategy = LeveledCompactionStrategy(
                    maxLevels = 3,
                    level0FileNumberCompactionTrigger = 2,
                    levelSizeMultiplier = 10.0,
                    fileSelectionStrategy = BY_MOST_DELETIONS,
                    baseLevelMinSize = 100.MiB,
                )
            )

            // inspect the merges which have been executed by the compaction
            expectThat(store.executedMerges).single().and {
                get { this.keepTombstones }.isFalse()
                get { this.fileIndices }.containsExactlyInAnyOrder(4, 7)
                get { this.trigger }.isEqualTo(CompactionTrigger.LEVELED_TARGET_SIZE_RATIO)
            }
        }
    }

    @ParameterizedTest
    @VirtualFileSystemTest
    fun canMergeFilesUpwards(mode: VFSMode) {
        mode.withVFS { vfs ->
            // set up a fake store with some files
            vfs.createManifestFile()
            val store = vfs.createFakeLeveledStore {
                level(0) {
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
            store.executeLeveledCompactionSynchronously(
                strategy = LeveledCompactionStrategy(
                    maxLevels = 3,
                    level0FileNumberCompactionTrigger = 2,
                    levelSizeMultiplier = 10.0,
                    fileSelectionStrategy = BY_MOST_DELETIONS,
                    baseLevelMinSize = 100.MiB,
                )
            )

            // inspect the merges which have been executed by the compaction
            expectThat(store.executedMerges).single().and {
                get { this.keepTombstones }.isFalse()
                get { this.fileIndices }.containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7)
                get { this.trigger }.isEqualTo(CompactionTrigger.LEVELED_LEVEL0)
            }
        }
    }

    // =================================================================================================================
    // HELPER FUNCTIONS
    // =================================================================================================================

    private fun VirtualFileSystem.createManifestFile(): ManifestFile {
        return ManifestFile(file(ManifestFile.FILE_NAME))
    }

    private fun DoubleArray.roundTo2Decimals(): DoubleArray {
        return this
            .map { (it * 100).toInt() / 100.0 }
            .toDoubleArray()
    }

}