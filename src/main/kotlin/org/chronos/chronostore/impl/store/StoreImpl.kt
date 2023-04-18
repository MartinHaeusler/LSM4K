package org.chronos.chronostore.impl.store

import org.chronos.chronostore.api.Store
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.datablock.BlockReadMode
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.lsm.LocalBlockCache
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.lsm.MergeStrategy
import org.chronos.chronostore.util.StoreId

class StoreImpl(
    override val id: StoreId,
    override var name: String,
    override val retainOldVersions: Boolean,
    override val directory: VirtualDirectory,
    blockReadMode: BlockReadMode,
    mergeStrategy: MergeStrategy,
    blockCache: LocalBlockCache,
    driverFactory: RandomFileAccessDriverFactory,
) : Store {

    val tree = LSMTree(this.directory, mergeStrategy, blockCache, driverFactory, blockReadMode)

}