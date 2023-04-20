package org.chronos.chronostore.impl.store

import org.chronos.chronostore.api.Store
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.datablock.BlockReadMode
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.lsm.LocalBlockCache
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.lsm.MergeStrategy
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId

class StoreImpl(
    override val id: StoreId,
    override var name: String,
    override val retainOldVersions: Boolean,
    override val validFrom: Timestamp,
    override var validTo: Timestamp?,
    override val createdByTransactionId: TransactionId,
    override val directory: VirtualDirectory,
    blockReadMode: BlockReadMode,
    mergeStrategy: MergeStrategy,
    blockCache: LocalBlockCache,
    driverFactory: RandomFileAccessDriverFactory,
) : Store {

    val tree = LSMTree(this.directory, mergeStrategy, blockCache, driverFactory, blockReadMode)

}