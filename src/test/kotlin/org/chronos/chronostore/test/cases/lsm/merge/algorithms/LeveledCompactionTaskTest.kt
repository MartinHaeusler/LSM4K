package org.chronos.chronostore.test.cases.lsm.merge.algorithms

import org.chronos.chronostore.lsm.merge.algorithms.LeveledCompactionTask
import org.chronos.chronostore.util.unit.BinarySize.Companion.GiB
import org.chronos.chronostore.util.unit.BinarySize.Companion.MiB
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.contentEquals

class LeveledCompactionTaskTest {

    @Test
    fun canComputeTargetLevelSizes1() {
        expectThat(
            LeveledCompactionTask.computeTargetLevelSizes(
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
            LeveledCompactionTask.computeTargetLevelSizes(
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
            LeveledCompactionTask.computeTargetLevelSizes(
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

}