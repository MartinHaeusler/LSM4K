package org.chronos.chronostore.impl.store

import org.chronos.chronostore.api.Store
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.lsm.LocalBlockCache
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.lsm.merge.strategy.MergeService
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.unit.BinarySize

class StoreImpl(
    override val id: StoreId,
    override var name: String,
    override val retainOldVersions: Boolean,
    override val validFrom: Timestamp,
    override var validTo: Timestamp?,
    override val createdByTransactionId: TransactionId,
    override val directory: VirtualDirectory,
    mergeService: MergeService,
    blockCache: LocalBlockCache,
    driverFactory: RandomFileAccessDriverFactory,
    newFileSettings: ChronoStoreFileSettings,
    maxInMemoryTreeSize: BinarySize,
) : Store {

    val tree = LSMTree(
        directory = this.directory,
        mergeService = mergeService,
        blockCache = blockCache,
        driverFactory = driverFactory,
        newFileSettings = newFileSettings,
        maxInMemoryTreeSize = maxInMemoryTreeSize,
    )

    override fun toString(): String {
        return "Store[${this.name} (ID: ${this.id})]"
    }

}