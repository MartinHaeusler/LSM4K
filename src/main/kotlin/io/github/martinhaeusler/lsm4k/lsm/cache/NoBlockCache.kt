package io.github.martinhaeusler.lsm4k.lsm.cache

import io.github.martinhaeusler.lsm4k.io.format.BlockLoader
import io.github.martinhaeusler.lsm4k.io.format.datablock.DataBlock
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFile
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