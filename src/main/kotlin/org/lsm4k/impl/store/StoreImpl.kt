package org.lsm4k.impl.store

import org.lsm4k.api.Store
import org.lsm4k.impl.Killswitch
import org.lsm4k.io.fileaccess.RandomFileAccessDriverFactory
import org.lsm4k.io.format.LSMFileSettings
import org.lsm4k.io.vfs.VirtualDirectory
import org.lsm4k.lsm.LSMForestMemoryManager
import org.lsm4k.lsm.LSMTree
import org.lsm4k.lsm.cache.BlockCache
import org.lsm4k.lsm.cache.FileHeaderCache
import org.lsm4k.manifest.ManifestFile
import org.lsm4k.manifest.StoreMetadata
import org.lsm4k.util.StoreId
import org.lsm4k.util.TSN
import org.lsm4k.util.TransactionId
import org.lsm4k.util.statistics.StatisticsReporter
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
    statisticsReporter: StatisticsReporter,
    driverFactory: RandomFileAccessDriverFactory,
    newFileSettings: LSMFileSettings,
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
        statisticsReporter = statisticsReporter,
    )


    override fun scheduleMajorCompaction(): CompletableFuture<*> {
        return this.tree.scheduleMajorCompaction()
    }

    override fun scheduleMinorCompaction(): CompletableFuture<*> {
        return this.tree.scheduleMinorCompaction()
    }

    override fun scheduleMemtableFlush(scheduleMinorCompactionOnCompletion: Boolean): CompletableFuture<*> {
        return this.tree.scheduleMemtableFlush(scheduleMinorCompactionOnCompletion)
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