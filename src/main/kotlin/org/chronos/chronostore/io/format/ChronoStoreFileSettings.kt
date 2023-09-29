package org.chronos.chronostore.io.format

import org.chronos.chronostore.util.unit.BinarySize

class ChronoStoreFileSettings(
    /** The compression used for the blocks in the file. */
    val compression: CompressionAlgorithm,
    /** The maximum size of a single block. */
    val maxBlockSize: BinarySize,
)