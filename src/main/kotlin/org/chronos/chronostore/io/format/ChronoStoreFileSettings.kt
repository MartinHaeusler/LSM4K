package org.chronos.chronostore.io.format

import org.chronos.chronostore.util.unit.BinarySize

class ChronoStoreFileSettings(
    /** The compression used for the blocks in the file. */
    val compression: CompressionAlgorithm,
    /** The maximum size of a single block. */
    val maxBlockSize: BinarySize,
    /**
     * Every N-th key per block will be indexed.
     *
     * For example, if [indexRate] = 10, every 10th key will be
     * added to the block index.
     *
     * The first and the last entry of the block will always
     * be part of the index.
     */
    val indexRate: Int,
) {

}