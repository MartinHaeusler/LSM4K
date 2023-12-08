package org.chronos.chronostore.checkpoint

import org.chronos.chronostore.util.Timestamp

data class CheckpointData(
    val lowWatermark: Timestamp,
)
