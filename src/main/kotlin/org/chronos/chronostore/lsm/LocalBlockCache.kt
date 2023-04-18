package org.chronos.chronostore.lsm

import org.chronos.chronostore.io.format.datablock.DataBlock

interface LocalBlockCache {

    fun getBlock(blockOffset: Int, loader: (Int) -> DataBlock?): DataBlock?

    companion object {

        val NONE = NoBlockCache

    }

}