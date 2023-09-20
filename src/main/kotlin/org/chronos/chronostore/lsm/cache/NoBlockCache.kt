package org.chronos.chronostore.lsm.cache

import org.chronos.chronostore.io.format.datablock.DataBlock
import java.util.*

object NoBlockCache: LocalBlockCache {

    override fun getBlock(fileId: UUID, blockIndex: Int, loader: (Int) -> DataBlock?): DataBlock? {
        return loader(blockIndex)
    }

}