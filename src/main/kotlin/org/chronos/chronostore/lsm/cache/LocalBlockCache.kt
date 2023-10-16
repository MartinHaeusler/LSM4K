package org.chronos.chronostore.lsm.cache

import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.util.StoreId
import java.util.*

sealed interface LocalBlockCache {

    val storeId: StoreId?

    fun getBlock(fileId: UUID, blockIndex: Int, loader: (Int) -> DataBlock?): DataBlock?

    companion object {

        val NONE = NoBlockCache

    }

}