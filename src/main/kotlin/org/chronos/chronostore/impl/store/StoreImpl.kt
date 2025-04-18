package org.chronos.chronostore.impl.store

import org.chronos.chronostore.api.Store
import org.chronos.chronostore.impl.Killswitch
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.lsm.LSMForestMemoryManager
import org.chronos.chronostore.lsm.LSMTree
import org.chronos.chronostore.lsm.cache.BlockCache
import org.chronos.chronostore.lsm.cache.FileHeaderCache
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.TransactionId
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor

class StoreImpl(
    override val directory: VirtualDirectory,
    compactionExecutor: Executor,
    memtableFlushExecutor: Executor,
    manifestFile: ManifestFile,
    initialStoreMetadata: StoreMetadata,
    forest: LSMForestMemoryManager,
    blockCache: BlockCache,
    fileHeaderCache: FileHeaderCache,
    driverFactory: RandomFileAccessDriverFactory,
    newFileSettings: ChronoStoreFileSettings,
    getSmallestOpenReadTSN: () -> TSN?,
    killswitch: Killswitch,
) : Store {

    override val storeId: StoreId = initialStoreMetadata.storeId
    override val validFromTSN: TSN = initialStoreMetadata.info.validFromTSN
    override var validToTSN: TSN? = initialStoreMetadata.info.validToTSN
    override val createdByTransactionId: TransactionId = initialStoreMetadata.info.createdByTransactionId


    val tree = LSMTree(
        storeId = this.storeId,
        forest = forest,
        compactionExecutor = compactionExecutor,
        memtableFlushExecutor = memtableFlushExecutor,
        manifestFile = manifestFile,
        directory = this.directory,
        blockCache = blockCache,
        driverFactory = driverFactory,
        newFileSettings = newFileSettings,
        fileHeaderCache = fileHeaderCache,
        getSmallestOpenReadTSN = getSmallestOpenReadTSN,
        initialStoreMetadata = initialStoreMetadata,
        killswitch = killswitch,
    )


    override fun scheduleMajorCompaction(): CompletableFuture<*> {
        return this.tree.scheduleMajorCompaction()
    }

    override fun scheduleMinorCompaction(): CompletableFuture<*> {
        return this.tree.scheduleMinorCompaction()
    }

    override fun scheduleMemtableFlush(): CompletableFuture<*> {
        return this.tree.scheduleMemtableFlush()
    }

    override val highWatermarkTSN: TSN?
        get() = this.tree.latestReceivedCommitTSN

    override val lowWatermarkTSN: TSN?
        get() = this.tree.latestPersistedCommitTSN

    override fun hasInMemoryChanges(): Boolean {
        return this.tree.inMemorySize.bytes > 0L
    }

    override fun toString(): String {
        return "Store[${this.storeId}]"
    }

}