package org.chronos.chronostore.lsm.cache

import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import java.util.*

data object NoBlockCache: LocalBlockCache {

    override val storeId: StoreId?
        get() = null

    override fun getBlock(fileId: UUID, blockIndex: Int, loader: (Int) -> DataBlock?): DataBlock? {
        ChronoStoreStatistics.BLOCK_CACHE_REQUESTS.incrementAndGet()
        ChronoStoreStatistics.BLOCK_CACHE_MISSES.incrementAndGet()
        return loader(blockIndex)
    }

}