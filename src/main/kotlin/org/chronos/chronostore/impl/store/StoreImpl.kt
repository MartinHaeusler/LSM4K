package org.chronos.chronostore.impl.store

import org.chronos.chronostore.api.Store
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.lsm.LSMForestMemoryManager
import org.chronos.chronostore.lsm.cache.LocalBlockCache
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.lsm.cache.FileHeaderCache
import org.chronos.chronostore.lsm.merge.strategy.MergeService
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId

class StoreImpl(
    override val name: StoreId,
    override val retainOldVersions: Boolean,
    override val validFrom: Timestamp,
    override var validTo: Timestamp?,
    override val createdByTransactionId: TransactionId,
    override val directory: VirtualDirectory,
    forest: LSMForestMemoryManager,
    mergeService: MergeService,
    blockCache: LocalBlockCache,
    fileHeaderCache: FileHeaderCache,
    driverFactory: RandomFileAccessDriverFactory,
    newFileSettings: ChronoStoreFileSettings,
) : Store {

    val tree = LSMTree(
        forest = forest,
        directory = this.directory,
        mergeService = mergeService,
        blockCache = blockCache,
        driverFactory = driverFactory,
        newFileSettings = newFileSettings,
        fileHeaderCache = fileHeaderCache,
    )

    override fun toString(): String {
        return "Store[${this.name}]"
    }

}