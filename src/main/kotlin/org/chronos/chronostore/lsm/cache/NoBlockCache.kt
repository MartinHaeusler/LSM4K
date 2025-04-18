package org.chronos.chronostore.lsm.cache

import org.chronos.chronostore.io.format.BlockLoader
import org.chronos.chronostore.io.format.datablock.DataBlock
import org.chronos.chronostore.io.vfs.VirtualFile
import java.util.concurrent.CompletableFuture

class NoBlockCache(
    private val loader: BlockLoader,
) : BlockCache {

    override fun getBlockAsync(file: VirtualFile, blockIndex: Int): CompletableFuture<DataBlock?> {
        return this.loader.getBlockAsync(file, blockIndex)
    }

    override val isAsyncSupported: Boolean
        get() = this.loader.isAsyncSupported

}