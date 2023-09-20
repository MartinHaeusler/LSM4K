package org.chronos.chronostore.lsm.cache

import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.lsm.cache.NoBlockCache
import java.util.UUID

interface LocalBlockCache {

    fun getBlock(fileId: UUID, blockIndex: Int, loader: (Int) -> DataBlock?): DataBlock?

    companion object {

        val NONE = NoBlockCache

    }

}