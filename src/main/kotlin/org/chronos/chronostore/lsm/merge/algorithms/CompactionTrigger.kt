package org.chronos.chronostore.lsm.merge.algorithms

enum class CompactionTrigger {

    DATA_AGE,

    SPACE_AMPLIFICATION,

    SIZE_RATIO,

    FULL_COMPACTION,


}