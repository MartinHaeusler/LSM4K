package org.chronos.chronostore.impl.store

import org.chronos.chronostore.api.Store
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.lsm.LSMForestMemoryManager
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.lsm.cache.FileHeaderCache
import org.chronos.chronostore.lsm.cache.LocalBlockCache
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.TransactionId

class StoreImpl(
    override val directory: VirtualDirectory,
    initialStoreMetadata: StoreMetadata,
    forest: LSMForestMemoryManager,
    blockCache: LocalBlockCache,
    fileHeaderCache: FileHeaderCache,
    driverFactory: RandomFileAccessDriverFactory,
    newFileSettings: ChronoStoreFileSettings,
    getSmallestOpenReadTSN: () -> TSN?,
) : Store {

    override val storeId: StoreId = initialStoreMetadata.storeId
    override val validFromTSN: TSN = initialStoreMetadata.info.validFromTSN
    override var validToTSN: TSN? = initialStoreMetadata.info.validToTSN
    override val createdByTransactionId: TransactionId = initialStoreMetadata.info.createdByTransactionId


    val tree = LSMTree(
        storeId = this.storeId,
        forest = forest,
        directory = this.directory,
        blockCache = blockCache,
        driverFactory = driverFactory,
        newFileSettings = newFileSettings,
        fileHeaderCache = fileHeaderCache,
        getSmallestOpenReadTSN = getSmallestOpenReadTSN,
        initialStoreMetadata = initialStoreMetadata,
    )

    override val highWatermarkTSN: TSN?
        get() = this.tree.latestReceivedCommitTSN

    override val lowWatermarkTSN: TSN?
        get() {
            if (!this.hasInMemoryChanges()) {
                // we have no in-memory changes, therefore ALL data belonging
                // to this store has been persisted. We have nothing to
                // contribute to the low watermark.
                return null
            }
            return this.tree.latestPersistedCommitTSN
        }

    override fun hasInMemoryChanges(): Boolean {
        return this.tree.inMemorySize.bytes > 0L
    }

    override fun toString(): String {
        return "Store[${this.storeId}]"
    }

}