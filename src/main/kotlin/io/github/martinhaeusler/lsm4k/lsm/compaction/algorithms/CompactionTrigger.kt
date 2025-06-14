package io.github.martinhaeusler.lsm4k.lsm.compaction.algorithms

enum class CompactionTrigger {

    TIER_SPACE_AMPLIFICATION,

    TIER_SIZE_RATIO,

    TIER_HEIGHT_REDUCTION,

    LEVELED_LEVEL0,

    LEVELED_TARGET_SIZE_RATIO,

    FULL_COMPACTION,


}