package org.chronos.chronostore.lsm.cache

import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.util.StoreId
import java.util.*

object NoBlockCache: LocalBlockCache {

    override val storeId: StoreId?
        get() = null

    override fun getBlock(fileId: UUID, blockIndex: Int, loader: (Int) -> DataBlock?): DataBlock? {
        return loader(blockIndex)
    }

}