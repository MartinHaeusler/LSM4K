package org.chronos.chronostore.io.format

import org.chronos.chronostore.util.unit.BinarySize

class ChronoStoreFileSettings(
    /** The compression used for the blocks in the file. */
    val compression: CompressionAlgorithm,
    /** The maximum size of a single block. */
    val maxBlockSize: BinarySize,
) {

    val sizeBytes: Int
        get(){
            return Int.SIZE_BYTES +
                // size of "maxBlockSize"
                Int.SIZE_BYTES +
                Long.SIZE_BYTES
        }

    override fun toString(): String {
        return "ChronoStoreFileSettings(compression=$compression, maxBlockSize=$maxBlockSize)"
    }
}