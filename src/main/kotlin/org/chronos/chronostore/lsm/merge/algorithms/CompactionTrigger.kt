package org.chronos.chronostore.lsm.merge.algorithms

enum class CompactionTrigger {

    TIER_SPACE_AMPLIFICATION,

    TIER_SIZE_RATIO,

    TIER_HEIGHT_REDUCTION,

    FULL_COMPACTION,


}