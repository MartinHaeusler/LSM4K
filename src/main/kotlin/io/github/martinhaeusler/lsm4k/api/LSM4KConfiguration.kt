package io.github.martinhaeusler.lsm4k.api

import com.cronutils.model.Cron
import io.github.martinhaeusler.lsm4k.api.compaction.CompactionStrategy
import io.github.martinhaeusler.lsm4k.compressor.api.Compressor
import io.github.martinhaeusler.lsm4k.io.fileaccess.*
import io.github.martinhaeusler.lsm4k.io.vfs.disk.DiskBasedVirtualFileSystemSettings
import io.github.martinhaeusler.lsm4k.io.vfs.disk.FileSyncMode
import io.github.martinhaeusler.lsm4k.util.cron.CronUtils
import io.github.martinhaeusler.lsm4k.util.cron.CronUtils.cron
import io.github.martinhaeusler.lsm4k.util.cron.CronUtils.isValid
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.Bytes
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.MiB
import io.github.martinhaeusler.lsm4k.util.unit.SizeUnit
import java.lang.foreign.MemorySegment
import java.nio.channels.FileChannel
import kotlin.math.max
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toKotlinDuration

class LSM4KConfiguration(

    /**
     * The maximum number of threads to use for executing compactions.
     *
     * Each individual store may only have a single concurrent compaction running at any point in time, but if there
     * are multiple stores, there can be multiple concurrent compactions (at most one per store). This setting controls
     * how many parallel compactions there can be at most, regardless of the number of stores.
     *
     * A higher number means that more stores can perform background compaction work simultaneously, but it also means
     * that more I/O throughput will be dedicated to those operations (rather than queries or flushes).
     */
    val maxCompactionThreads: Int = DEFAULT_MAX_COMPACTION_THREADS,

    /**
     * the maximum number of threads to use for executing memtable flush tasks.
     *
     * Each store has a memtable which gradually fills up with data as transactions on it are committed. When the memtable
     * is full, it needs to be written to disk to avoid out-of-memory issues. While this is a pressing concern, having too
     * many concurrent flush tasks can overwhelm the disk and consume large amounts of IO operations. This setting allows
     * to limit the maximum number of concurrent flush tasks. Please note that regardless of this setting, each store only
     * allows for up to one flush task at the same time.
     *
     * A higher number means that more stores can flush their memtables simultaneously, but it also means
     * that more I/O throughput will be dedicated to those operations (rather than queries or compactions).
     */
    val maxMemtableFlushThreads: Int = DEFAULT_MAX_MEMTABLE_FLUSH_THREADS,

    /**
     * When writing new files: the compression algorithm to use. All old files will remain readable if this setting is changed.
     *
     * Please note that the corresponding dependency must be added to the classpath when using other compression algorithms,
     * otherwise runtime errors will occur!
     */
    val compressionAlgorithm: String = DEFAULT_COMPRESSION_ALGORITHM,

    /**
     * The maximum (uncompressed) size of a single data block.
     *
     * This setting is only taken into account when new blocks are written; it does not affect the existing persistent data.
     *
     * This setting is a **soft limit**. In certain situations, individual blocks may be larger if very large key-value pairs
     * are being stored.
     */
    val maxBlockSize: BinarySize = DEFAULT_MAX_BLOCK_SIZE,

    /**
     * The factory which produces the [RandomFileAccessDriver]s.
     *
     * This primarily decides the access pattern for random-access files.
     *
     * You can find the driver factory for a driver class using `<DriverClass>.Factory`.
     *
     * Currently supported driver classes include:
     *
     * - [FileChannelDriver.Factory] uses a [FileChannel] to fetch data from disk. This is the default driver for on-disk files.
     * - [InMemoryFileDriver.Factory] is always used when LSM4K is started in in-memory mode (even when this setting is set to something else).
     *
     * In addition, the following options are supported for advanced use cases:
     *
     * - [MemoryMappedFileDriver.Factory] uses memory-mapped files (`mmap`) for fast access.
     * - [MemorySegmentFileDriver.Factory] uses the [MemorySegment] API (part of the Java Foreign Memory API) for memory-mapped access.
     */
    val randomFileAccessDriverFactory: RandomFileAccessDriverFactory = DEFAULT_RANDOM_FILE_ACCESS_DRIVER_FACTORY,

    /**
     * The default compaction strategy to employ for all newly created stores.
     *
     * Individual stores may override this setting. Existing stores will not be altered if this setting changes.
     */
    val defaultCompactionStrategy: CompactionStrategy = DEFAULT_COMPACTION_STRATEGY,

    /**
     * The timing for time-triggered minor compactions.
     *
     * Minor compactions only compact a small fraction of the files in each store and should be scheduled
     * fairly frequently (multiple times per hour). Minor compactions are additionally also triggered
     * automatically when new data is committed.
     *
     * Use `null` to disable time-based minor compaction. This is not recommended, as periodic compaction is critical
     * for good read performance.
     */
    val minorCompactionCron: Cron? = DEFAULT_MINOR_COMPACTION_CRON,

    /**
     * The timing for time-triggered major compactions.
     *
     * Major compactions compact all files in all stores and are therefore expensive operations in terms
     * of all sorts of resources (I/O operations, memory utilization, CPU utilization). A major compaction
     * also takes a considerable amount of time; how much exactly depends on the amount of data in each store.
     * As a general rule of thumb, major compactions should be performed when the system is otherwise idle
     * (e.g. during nighttime) and not when the system is under heavy load. How frequent major compactions
     * need to occur depends on how much data gets written to the store.
     *
     * Use `null` to disable time-based major compaction. This is not recommended, as periodic compaction is critical
     * for good read performance.
     */
    val majorCompactionCron: Cron? = DEFAULT_MAJOR_COMPACTION_CRON,

    /**
     * The maximum size of the in-memory trees.
     *
     * If this value is exceeded by an insert, the insert is blocked (stalled) until flush tasks have freed memory.
     *
     * Please note that this is a **soft** limit. If commits are particularly large, this limit may be exceeded
     * temporarily to fit the commit data.
     */
    val maxForestSize: BinarySize = DEFAULT_MAX_FOREST_SIZE,

    /**
     * This fraction of [maxForestSize] determines when we start flushing to disk.
     *
     * Must be greater than zero and less than 1.0.
     */
    val forestFlushThreshold: Double = DEFAULT_FOREST_FLUSH_THRESHOLD,

    /**
     * A [Cron] expression which controls when - and how often - checkpoints should be performed.
     *
     * A checkpoint allows the LSM4K to restart quickly after a shutdown. The more data
     * was changed between the last checkpoint and the shutdown, the longer the recovery
     * will take when the LSM4K instance is restarted again.
     *
     * The tasks carried out by a checkpoint involve:
     *
     *  - Checking what the highest persisted timestamp is per store
     *  - Based on that information, deleting old Write-Ahead-Log files which are no longer needed
     *  - Writing files that allow for faster recovery after a restart
     *
     * Use `null` to disable checkpoints. **WARNING:** disabling checkpoints
     * can lead to very large WAL files, slow LSM4K restart/recovery and increased
     * disk footprint!
     */
    val checkpointCron: Cron? = DEFAULT_CHECKPOINT_CRON,

    /**
     * The timing at which old store files should be garbage collected.
     *
     * This only affects on-disk files and has no impact on in-memory data structures or caches.
     *
     * It is recommended to run this task at least once per day.
     *
     * Use `null` to disable garbage collection. **WARNING:** disabling garbage collection
     * can lead to a lot of unused files and increased disk footprint!
     */
    val garbageCollectionCron: Cron? = DEFAULT_GARBAGE_COLLECTION_CRON,

    /**
     * The maximum size of the block cache to use.
     *
     * Each data file contains a list of data blocks. These blocks are
     * stored one after another and are typically [compressed][compressionAlgorithm].
     * Since it is very likely that a single data block will be accessed more
     * than once, and loading them from disk is a comparably expensive operation,
     * the block cache allows the store to keep a certain amount of blocks in-memory
     * to avoid constant re-fetching.
     *
     * The individual block size is solely determined by the keys and values
     * contained in the block. The maximum size of a single block is given
     * by [maxBlockSize], though this value may be exceeded by individual blocks
     * when dealing with very large individual keys and/or values.
     *
     * By default, 25% of the JVM heap space is used.
     *
     * Use `null` to disable the cache.
     */
    val blockCacheSize: BinarySize? = DEFAULT_BLOCK_CACHE_SIZE,

    /**
     * The maximum size of the file header cache to use.
     *
     * Each data file has a predefined section which is known as the "header",
     * containing metadata and index information. The data in a file can only
     * be loaded after the header has been processed. The file header cache
     * allows the store to keep some of the file headers in-memory to avoid
     * constant re-fetching.
     *
     * File header size is primarily determined by the number of keys in the
     * file (logarithmically) and the average size of a single key (linearly).
     *
     * By default, 1% of the JVM heap space is used.
     *
     * Use `null` to disable the cache.
     */
    val fileHeaderCacheSize: BinarySize? = DEFAULT_FILE_HEADER_CACHE_SIZE,

    /**
     * Determines how many threads are used for prefeching data from the persistence into the block cache.
     *
     * More prefetchers generally mean faster read access in cursors, but this gain will
     * eventually be capped by the maximum parallelism supported by the hardware of the
     * hard drive. SSDs generally allow for higher parallelism than HDDs. If this number
     * exceeds the hardware capabilities, performance may drop.
     *
     * By default, half the available processors are used (but at least 1).
     *
     * Minimum is 1.
     */
    val prefetchingThreads: Int = DEFAULT_PREFETCHING_THREADS,

    /**
     * The maximum disk footprint of a single Write-Ahead-Log file.
     *
     * Please note that this is a **soft limit**. No more data may be written
     * to a Write-Ahead-Log file if it reaches this limit, but transactions are
     * always written as a whole into these files. If a transaction happens to
     * be very large, the file will be made larger to accommodate the contents
     * of the transaction.
     *
     * Write-Ahead-Log files are cleaned up over time and do not accumulate
     * indefinitely.
     *
     * Defaults to 128 MiB, must be greater than 0.
     */
    val maxWriteAheadLogFileSize: BinarySize = DEFAULT_MAX_WRITE_AHEAD_LOG_FILE_SIZE,

    /**
     * The sync mode for files.
     *
     * This mode determines how the files produced by the store are synced to disk.
     * Please refer to the individual enum values for details.
     *
     * This setting will only take effect if disk-based persistence is used.
     */
    val fileSyncMode: FileSyncMode = DEFAULT_FILE_SYNC_MODE,

    /**
     * The minimum number of Write-Ahead-Log (WAL) files to keep.
     *
     * If the number of WAL files is less than or equal to this setting,
     * WAL shortening will be skipped.
     */
    val minNumberOfWriteAheadLogFiles: Int = DEFAULT_MIN_NUMBER_OF_WRITE_AHEAD_LOG_FILES,

    /**
     * Determines if LSM4K should attempt to create a checkpoint during store shutdown.
     *
     * Checkpoints are taken regularly (see: [checkpointCron]) and speed up the restart of the
     * store. By taking a checkpoint during shutdown, we guarantee that a recent checkpoint is
     * available during restart.
     *
     * Disable this option if the time-to-shutdown is more important than the time-to-restart.
     */
    val checkpointOnShutdown: Boolean = DEFAULT_CHECKPOINT_ON_SHUTDOWN,

    /**
     * The maximum size of the buffer when replaying the write ahead log (WAL).
     *
     * A larger buffer results in faster WAL replay at startup.
     *
     * Please note that this is a **soft** limit. If a single key-value pair is very large,
     * this limit may be temporarily exceeded.
     */
    val walBufferSize: BinarySize = DEFAULT_WAL_BUFFER_SIZE,

    /**
     * When opening a [read-write][DatabaseEngine.beginReadWriteTransaction] or [exclusive][DatabaseEngine.beginExclusiveTransaction] exception,
     * this argument determines the default timeout to wait for the required lock.
     *
     * This value can be overridden on a per-transaction basis by passing an explicit argument to the [DatabaseEngine.beginReadWriteTransaction] / [DatabaseEngine.beginExclusiveTransaction]
     * method.
     */
    val defaultLockAcquisitionTimeout: Duration = DEFAULT_LOCK_ACQUISITION_TIMEOUT,

    ) {


    companion object {

        // =================================================================================================================
        // DEFAULT SETTINGS
        // =================================================================================================================

        private val DEFAULT_MAX_COMPACTION_THREADS: Int = 4

        private val DEFAULT_MAX_MEMTABLE_FLUSH_THREADS: Int = 4

        private val DEFAULT_COMPRESSION_ALGORITHM: String = "gzip"

        private val DEFAULT_MAX_BLOCK_SIZE: BinarySize = 8.MiB

        private val DEFAULT_RANDOM_FILE_ACCESS_DRIVER_FACTORY: RandomFileAccessDriverFactory = FileChannelDriver.Factory

        private val DEFAULT_COMPACTION_STRATEGY: CompactionStrategy = CompactionStrategy.DEFAULT

        private val DEFAULT_MINOR_COMPACTION_CRON: Cron? = cron("0 */10 * * * *") // every 10 minutes

        private val DEFAULT_MAJOR_COMPACTION_CRON: Cron? = cron("0 0 2 * * 0") // at 02:00 every sunday

        private val DEFAULT_MAX_FOREST_SIZE: BinarySize = 250.MiB

        private val DEFAULT_FOREST_FLUSH_THRESHOLD: Double = 0.33

        private val DEFAULT_CHECKPOINT_CRON: Cron? = cron("0 */10 * * * *") // every 10 minutes

        private val DEFAULT_GARBAGE_COLLECTION_CRON: Cron? = cron("0 */10 * * * *") // every 10 minutes

        private val DEFAULT_BLOCK_CACHE_SIZE: BinarySize? = (Runtime.getRuntime().maxMemory() / 4).toInt().Bytes

        private val DEFAULT_FILE_HEADER_CACHE_SIZE: BinarySize? = (Runtime.getRuntime().maxMemory() / 100).toInt().Bytes

        private val DEFAULT_PREFETCHING_THREADS: Int = max((Runtime.getRuntime().availableProcessors() / 2), 1)

        private val DEFAULT_MAX_WRITE_AHEAD_LOG_FILE_SIZE: BinarySize = 128.MiB

        private val DEFAULT_FILE_SYNC_MODE: FileSyncMode = FileSyncMode.CHANNEL_DATASYNC

        private val DEFAULT_MIN_NUMBER_OF_WRITE_AHEAD_LOG_FILES: Int = 3

        private val DEFAULT_CHECKPOINT_ON_SHUTDOWN: Boolean = true

        private val DEFAULT_WAL_BUFFER_SIZE: BinarySize = 128.MiB

        private val DEFAULT_LOCK_ACQUISITION_TIMEOUT: Duration = 10.seconds

        // =================================================================================================================
        // BUILDER STARTER
        // =================================================================================================================

        @JvmStatic
        fun builder(): Builder {
            return Builder()
        }

    }


    init {
        require(this.maxWriteAheadLogFileSize.bytes > 0) { "Cannot use a negative value for 'maxWriteAheadLogFileSize'!" }
        require(this.checkpointCron?.isValid() ?: true) { "The cron expression for 'writeAheadLogCompactionCron' is invalid: ${this.checkpointCron}" }
    }

    fun createVirtualFileSystemConfiguration(): DiskBasedVirtualFileSystemSettings {
        return DiskBasedVirtualFileSystemSettings(
            fileSyncMode = this.fileSyncMode
        )
    }


    class Builder {

        private var maxCompactionThreads: Int = DEFAULT_MAX_COMPACTION_THREADS

        private var maxMemtableFlushThreads: Int = DEFAULT_MAX_MEMTABLE_FLUSH_THREADS

        private var compressionAlgorithm: String = DEFAULT_COMPRESSION_ALGORITHM

        private var maxBlockSize: BinarySize = DEFAULT_MAX_BLOCK_SIZE

        private var randomFileAccessDriverFactory: RandomFileAccessDriverFactory = DEFAULT_RANDOM_FILE_ACCESS_DRIVER_FACTORY

        private var defaultCompactionStrategy: CompactionStrategy = DEFAULT_COMPACTION_STRATEGY

        private var minorCompactionCron: Cron? = DEFAULT_MINOR_COMPACTION_CRON

        private var majorCompactionCron: Cron? = DEFAULT_MAJOR_COMPACTION_CRON

        private var maxForestSize: BinarySize = DEFAULT_MAX_FOREST_SIZE

        private var forestFlushThreshold: Double = DEFAULT_FOREST_FLUSH_THRESHOLD

        private var checkpointCron: Cron? = DEFAULT_CHECKPOINT_CRON

        private var garbageCollectionCron: Cron? = DEFAULT_GARBAGE_COLLECTION_CRON

        private var blockCacheSize: BinarySize? = DEFAULT_BLOCK_CACHE_SIZE

        private var fileHeaderCacheSize: BinarySize? = DEFAULT_FILE_HEADER_CACHE_SIZE

        private var prefetchingThreads: Int = DEFAULT_PREFETCHING_THREADS

        private var maxWriteAheadLogFileSize: BinarySize = DEFAULT_MAX_WRITE_AHEAD_LOG_FILE_SIZE

        private var fileSyncMode: FileSyncMode = DEFAULT_FILE_SYNC_MODE

        private var minNumberOfWriteAheadLogFiles: Int = DEFAULT_MIN_NUMBER_OF_WRITE_AHEAD_LOG_FILES

        private var checkpointOnShutdown: Boolean = DEFAULT_CHECKPOINT_ON_SHUTDOWN

        private var walBufferSize: BinarySize = DEFAULT_WAL_BUFFER_SIZE

        private var defaultLockAcquisitionTimeout: Duration = DEFAULT_LOCK_ACQUISITION_TIMEOUT


        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.maxCompactionThreads]
         */
        fun withMaxCompactionThreads(maxCompactionThreads: Int): Builder {
            this.maxCompactionThreads = maxCompactionThreads
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.maxMemtableFlushThreads]
         */
        fun withMaxMemtableFlushThreads(maxMemtableFlushThreads: Int): Builder {
            this.maxMemtableFlushThreads = maxMemtableFlushThreads
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.compressionAlgorithm]
         */
        fun withCompressionAlgorithm(compressionAlgorithm: String): Builder {
            this.compressionAlgorithm = compressionAlgorithm
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.compressionAlgorithm]
         */
        fun withCompressionAlgorithm(compressor: Compressor): Builder {
            return this.withCompressionAlgorithm(compressor.uniqueName)
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.maxBlockSize]
         */
        fun withMaxBlockSize(maxBlockSize: BinarySize): Builder {
            this.maxBlockSize = maxBlockSize
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.maxBlockSize]
         */
        fun withMaxBlockSize(size: Long, unit: SizeUnit): Builder {
            return this.withMaxBlockSize(BinarySize(size, unit))
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.randomFileAccessDriverFactory]
         */
        fun withRandomFileAccessDriverFactory(randomFileAccessDriverFactory: RandomFileAccessDriverFactory): Builder {
            this.randomFileAccessDriverFactory = randomFileAccessDriverFactory
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.defaultCompactionStrategy]
         */
        fun withDefaultCompactionStrategy(defaultCompactionStrategy: CompactionStrategy): Builder {
            this.defaultCompactionStrategy = defaultCompactionStrategy
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.minorCompactionCron]
         */
        fun withMinorCompactionCron(minorCompactionCron: Cron?): Builder {
            this.minorCompactionCron = minorCompactionCron
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.minorCompactionCron]
         */
        fun withMinorCompactionCron(minorCompactionCron: String?): Builder {
            return this.withMinorCompactionCron(minorCompactionCron?.let(CronUtils::cron))
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.majorCompactionCron]
         */
        fun withMajorCompactionCron(majorCompactionCron: Cron?): Builder {
            this.majorCompactionCron = majorCompactionCron
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.majorCompactionCron]
         */
        fun withMajorCompactionCron(majorCompactionCron: String?): Builder {
            return withMajorCompactionCron(majorCompactionCron?.let(CronUtils::cron))
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.maxForestSize]
         */
        fun withMaxForestSize(maxForestSize: BinarySize): Builder {
            this.maxForestSize = maxForestSize
            return this
        }


        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.maxForestSize]
         */
        fun withMaxForestSize(size: Long, unit: SizeUnit): Builder {
            return this.withMaxForestSize(BinarySize(size, unit))
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.forestFlushThreshold]
         */
        fun withForestFlushThreshold(forestFlushThreshold: Double): Builder {
            this.forestFlushThreshold = forestFlushThreshold
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.checkpointCron]
         */
        fun withCheckpointCron(checkpointCron: Cron?): Builder {
            this.checkpointCron = checkpointCron
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.checkpointCron]
         */
        fun withCheckpointCron(checkpointCron: String?): Builder {
            return this.withCheckpointCron(checkpointCron?.let(CronUtils::cron))
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.garbageCollectionCron]
         */
        fun withGarbageCollectionCron(garbageCollectionCron: Cron?): Builder {
            this.garbageCollectionCron = garbageCollectionCron
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.garbageCollectionCron]
         */
        fun withGarbageCollectionCron(garbageCollectionCron: String?): Builder {
            return this.withGarbageCollectionCron(garbageCollectionCron?.let(CronUtils::cron))
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.blockCacheSize]
         */
        fun withBlockCacheSize(blockCacheSize: BinarySize?): Builder {
            this.blockCacheSize = blockCacheSize
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.blockCacheSize]
         */
        fun withBlockCacheSize(size: Long, unit: SizeUnit): Builder {
            return withBlockCacheSize(BinarySize(size, unit))
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.fileHeaderCacheSize]
         */
        fun withFileHeaderCacheSize(fileHeaderCacheSize: BinarySize?): Builder {
            this.fileHeaderCacheSize = fileHeaderCacheSize
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.fileHeaderCacheSize]
         */
        fun withFileHeaderCacheSize(size: Long, sizeUnit: SizeUnit): Builder {
            return this.withFileHeaderCacheSize(BinarySize(size, sizeUnit))
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.prefetchingThreads]
         */
        fun withPrefetchingThreads(prefetchingThreads: Int): Builder {
            this.prefetchingThreads = prefetchingThreads
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.maxWriteAheadLogFileSize]
         */
        fun withMaxWriteAheadLogFileSize(maxWriteAheadLogFileSize: BinarySize): Builder {
            this.maxWriteAheadLogFileSize = maxWriteAheadLogFileSize
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.maxWriteAheadLogFileSize]
         */
        fun withMaxWriteAheadLogFileSize(size: Long, unit: SizeUnit): Builder {
            return this.withMaxWriteAheadLogFileSize(BinarySize(size, unit))
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.fileSyncMode]
         */
        fun withFileSyncMode(fileSyncMode: FileSyncMode): Builder {
            this.fileSyncMode = fileSyncMode
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.minNumberOfWriteAheadLogFiles]
         */
        fun withMinNumberOfWriteAheadLogFiles(minNumberOfWriteAheadLogFiles: Int): Builder {
            this.minNumberOfWriteAheadLogFiles = minNumberOfWriteAheadLogFiles
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.checkpointOnShutdown]
         */
        fun withCheckpointOnShutdown(checkpointOnShutdown: Boolean): Builder {
            this.checkpointOnShutdown = checkpointOnShutdown
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.walBufferSize]
         */
        fun withWalBufferSize(walBufferSize: BinarySize): Builder {
            this.walBufferSize = walBufferSize
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.walBufferSize]
         */
        fun withWalBufferSize(size: Long, unit: SizeUnit): Builder {
            return this.withWalBufferSize(BinarySize(size, unit))
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.defaultLockAcquisitionTimeout]
         */
        fun withDefaultLockAcquisitionTimeout(defaultLockAcquisitionTimeout: Duration): Builder {
            this.defaultLockAcquisitionTimeout = defaultLockAcquisitionTimeout
            return this
        }

        /**
         * See: [org.lsm4k.api.LSM4KConfiguration.defaultLockAcquisitionTimeout]
         */
        fun withDefaultLockAcquisitionTimeout(defaultLockAcquisitionTimeout: java.time.Duration): Builder {
            return this.withDefaultLockAcquisitionTimeout(defaultLockAcquisitionTimeout.toKotlinDuration())
        }

        /**
         * Finalizes and builds the configuration.
         *
         * @return the configuration
         */
        fun build(): LSM4KConfiguration {
            return LSM4KConfiguration(
                maxCompactionThreads = this.maxCompactionThreads,
                maxMemtableFlushThreads = this.maxMemtableFlushThreads,
                compressionAlgorithm = this.compressionAlgorithm,
                maxBlockSize = this.maxBlockSize,
                randomFileAccessDriverFactory = this.randomFileAccessDriverFactory,
                defaultCompactionStrategy = this.defaultCompactionStrategy,
                minorCompactionCron = this.minorCompactionCron,
                majorCompactionCron = this.majorCompactionCron,
                maxForestSize = this.maxForestSize,
                forestFlushThreshold = this.forestFlushThreshold,
                checkpointCron = this.checkpointCron,
                garbageCollectionCron = this.garbageCollectionCron,
                blockCacheSize = this.blockCacheSize,
                fileHeaderCacheSize = this.fileHeaderCacheSize,
                prefetchingThreads = this.prefetchingThreads,
                maxWriteAheadLogFileSize = this.maxWriteAheadLogFileSize,
                fileSyncMode = this.fileSyncMode,
                minNumberOfWriteAheadLogFiles = this.minNumberOfWriteAheadLogFiles,
                checkpointOnShutdown = this.checkpointOnShutdown,
                walBufferSize = this.walBufferSize,
                defaultLockAcquisitionTimeout = this.defaultLockAcquisitionTimeout,
            )
        }

    }
}