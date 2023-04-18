package org.chronos.chronostore.lsm

import org.chronos.chronostore.io.format.datablock.DataBlock

object NoBlockCache: LocalBlockCache {

    override fun getBlock(blockOffset: Int, loader: (Int) -> DataBlock?): DataBlock? {
        return loader(blockOffset)
    }

}