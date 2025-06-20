package io.github.martinhaeusler.lsm4k.io.prefetcher

import io.github.martinhaeusler.lsm4k.io.fileaccess.RandomFileAccessDriverFactory
import io.github.martinhaeusler.lsm4k.io.format.BlockLoader
import io.github.martinhaeusler.lsm4k.io.format.LSMFileFormat
import io.github.martinhaeusler.lsm4k.io.format.datablock.DataBlock
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFile
import io.github.martinhaeusler.lsm4k.lsm.cache.BlockCache
import io.github.martinhaeusler.lsm4k.lsm.cache.FileHeaderCache
import io.github.martinhaeusler.lsm4k.util.ManagerState
import io.github.martinhaeusler.lsm4k.util.ResourceContext.Companion.using
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsReporter
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * A service that allows to request data blocks in an asynchronous way.
 *
 * The actual execution, the number of workers, etc. is hidden from the caller.
 *
 * Please note that the [BlockPrefetchingManager] uses the given [FileHeaderCache],
 * but it does **not** use the [BlockCache]. This is because the prefetcher
 * sits "behind" the block cache manager.
 */
class BlockPrefetchingManager(
    private val fileHeaderCache: FileHeaderCache,
    private val driverFactory: RandomFileAccessDriverFactory,
    private val executor: ExecutorService,
    private val statisticsReporter: StatisticsReporter,
) : BlockLoader, AutoCloseable {

    companion object {

        fun createIfNecessary(
            fileHeaderCache: FileHeaderCache,
            driverFactory: RandomFileAccessDriverFactory,
            prefetchingThreads: Int,
            statisticsReporter: StatisticsReporter,
        ): BlockPrefetchingManager? {
            if (prefetchingThreads <= 0) {
                // disable prefetching
                return null
            }
            return BlockPrefetchingManager(
                fileHeaderCache = fileHeaderCache,
                driverFactory = driverFactory,
                executor = Executors.newFixedThreadPool(prefetchingThreads),
                statisticsReporter = statisticsReporter,
            )
        }

    }

    private var state = ManagerState.OPEN

    private val ongoingRequests = ConcurrentHashMap<RequestKey, CompletableFuture<DataBlock?>>()

    /**
     * Prefetches a data block in a non-blocking asynchronous fashion.
     *
     * If multiple requests for the same data block are received, only one of them is actually carried out
     * and all requests will share the same (immutable) outcome.
     *
     * @param file The file to read the block from
     * @param blockIndex The index of the block to read
     *
     * @return A [CompletableFuture] for the block. May contain `null` if the block was not found in that file.
     */
    fun prefetch(file: VirtualFile, blockIndex: Int): CompletableFuture<DataBlock?> {
        this.state.checkOpen()
        val key = RequestKey(file.path, blockIndex)
        val ongoingRequest = this.ongoingRequests[key]
        if (ongoingRequest != null) {
            // somebody else is looking for this block as well -> share the result!
            return ongoingRequest
        }

        val blockLoader = InternalBlockLoader(
            file = file,
            blockIndex = blockIndex,
            fileHeaderCache = this.fileHeaderCache,
            driverFactory = this.driverFactory,
            statisticsReporter = this.statisticsReporter,
        )

        return CompletableFuture.supplyAsync(blockLoader::load, this.executor)
            .handle { dataBlock, exception ->
                // the request has been completed, remove the key
                this.ongoingRequests.remove(key)

                if (exception != null) {
                    throw exception
                } else {
                    return@handle dataBlock
                }
            }
    }

    override val isAsyncSupported: Boolean
        get() = true

    override fun getBlockAsync(file: VirtualFile, blockIndex: Int): CompletableFuture<DataBlock?> {
        return this.prefetch(file, blockIndex)
    }

    override fun close() {
        this.closeInternal(ManagerState.CLOSED)
    }

    fun closePanic() {
        this.closeInternal(ManagerState.PANIC)
    }

    private fun closeInternal(closeState: ManagerState) {
        if (this.state.isClosed()) {
            this.state = closeState
            return
        }
        this.state = closeState
        this.executor.shutdownNow()
    }

    private data class RequestKey(
        val path: String,
        val blockIndex: Int,
    )

    private class InternalBlockLoader(
        private val file: VirtualFile,
        private val blockIndex: Int,
        private val fileHeaderCache: FileHeaderCache,
        private val driverFactory: RandomFileAccessDriverFactory,
        private val statisticsReporter: StatisticsReporter,
    ) {

        fun load(): DataBlock? = using {
            val driver = driverFactory.createDriver(file).autoClose()
            val header = fileHeaderCache.getFileHeader(file) {
                LSMFileFormat.loadFileHeader(driver)
            }
            return LSMFileFormat.loadBlockFromFileOrNull(
                driver = driver,
                fileHeader = header,
                blockIndex = blockIndex,
                statisticsReporter = statisticsReporter,
            )
        }

    }

}