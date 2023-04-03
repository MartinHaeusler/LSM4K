package org.chronos.chronostore.io.format

class ChronoStoreFileSettings(
    /** The compression used for the blocks in the file. */
    val compression: CompressionAlgorithm,
    /** The maximum size of a single block, in bytes. */
    val maxBlockSizeInBytes: Int,
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