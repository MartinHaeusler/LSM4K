package org.chronos.chronostore.lsm

import org.chronos.chronostore.io.format.datablock.DataBlock

interface LocalBlockCache {

    fun getBlock(fileIndex: Int, blockOffset: Long, loader: (Long) -> DataBlock?): DataBlock?

    companion object {

        val NONE = NoBlockCache

    }

}