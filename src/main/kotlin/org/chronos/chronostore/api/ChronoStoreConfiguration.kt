package org.chronos.chronostore.api

import com.cronutils.model.Cron
import org.chronos.chronostore.api.compaction.CompactionStrategy
import org.chronos.chronostore.io.fileaccess.*
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystemSettings
import org.chronos.chronostore.io.vfs.disk.FileSyncMode
import org.chronos.chronostore.util.cron.CronUtils.cron
import org.chronos.chronostore.util.cron.CronUtils.isValid
import org.chronos.chronostore.util.unit.BinarySize
import org.chronos.chronostore.util.unit.BinarySize.Companion.Bytes
import org.chronos.chronostore.util.unit.BinarySize.Companion.MiB
import java.lang.foreign.MemorySegment
import java.nio.channels.FileChannel
import kotlin.math.max

class ChronoStoreConfiguration(

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
    val maxCompactionThreads: Int = 4,

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
    val maxMemtableFlushThreads: Int = 4,

    /** When writing new files: the compression algorithm to use. All old files will remain readable if this setting is changed.*/
    val compressionAlgorithm: CompressionAlgorithm = CompressionAlgorithm.SNAPPY,

    /**
     * The maximum (uncompressed) size of a single data block.
     *
     * This setting is only taken into account when new blocks are written; it does not affect the existing persistent data.
     *
     * This setting is a **soft limit**. In certain situations, individual blocks may be larger if very large key-value pairs
     * are being stored.
     */
    val maxBlockSize: BinarySize = 8.MiB,

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
     * - [InMemoryFileDriver.Factory] is always used when ChronoStore is started in in-memory mode (even when this setting is set to something else).
     *
     * In addition, the following options are supported for advanced use cases:
     *
     * - [MemoryMappedFileDriver.Factory] uses memory-mapped files (`mmap`) for fast access.
     * - [MemorySegmentFileDriver.Factory] uses the [MemorySegment] API (part of the Java Foreign Memory API) for memory-mapped access.
     */
    val randomFileAccessDriverFactory: RandomFileAccessDriverFactory = FileChannelDriver.Factory,

    /**
     * The default compaction strategy to employ for all newly created stores.
     *
     * Individual stores may override this setting. Existing stores will not be altered if this setting changes.
     */
    val defaultCompactionStrategy: CompactionStrategy = CompactionStrategy.DEFAULT,

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
    val minorCompactionCron: Cron? = cron("0 */10 * * * *"), // every 10 minutes

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
    val majorCompactionCron: Cron? = cron("0 0 2 * * 0"), // at 02:00 every sunday

    /**
     * The maximum size of the in-memory trees.
     *
     * If this value is exceeded by an insert, the insert is blocked (stalled) until flush tasks have freed memory.
     *
     * Please note that this is a **soft** limit. If commits are particularly large, this limit may be exceeded
     * temporarily to fit the commit data.
     */
    val maxForestSize: BinarySize = 250.MiB,

    /**
     * This fraction of [maxForestSize] determines when we start flushing to disk.
     *
     * Must be greater than zero and less than 1.0.
     */
    val forestFlushThreshold: Double = 0.33,

    /**
     * A [Cron] expression which controls when - and how often - checkpoints should be performed.
     *
     * A checkpoint allows the ChronoStore to restart quickly after a shutdown. The more data
     * was changed between the last checkpoint and the shutdown, the longer the recovery
     * will take when the ChronoStore instance is restarted again.
     *
     * The tasks carried out by a checkpoint involve:
     *
     *  - Checking what the highest persisted timestamp is per store
     *  - Based on that information, deleting old Write-Ahead-Log files which are no longer needed
     *  - Writing files that allow for faster recovery after a restart
     *
     * Use `null` to disable checkpoints. **WARNING:** disabling checkpoints
     * can lead to very large WAL files, slow ChronoStore restart/recovery and increased
     * disk footprint!
     */
    val checkpointCron: Cron? = cron("0 */10 * * * *"), // every 10 minutes

    /**
     * The maximum number of checkpoint files to keep.
     *
     * This value must be greater than or equal to 1. Keeping multiple checkpoint files
     * can help with recovery in case that the newer checkpoint files have been corrupted.
     */
    // TODO [CORRECTNESS] Is this really unused? Should it be used?
    val maxCheckpointFiles: Int = 5,

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
    val garbageCollectionCron: Cron? = cron("0 */10 * * * *"), // every 10 minutes

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
    val blockCacheSize: BinarySize? = (Runtime.getRuntime().maxMemory() / 4).toInt().Bytes,

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
    val fileHeaderCacheSize: BinarySize? = (Runtime.getRuntime().maxMemory() / 100).toInt().Bytes,

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
    val prefetchingThreads: Int = max((Runtime.getRuntime().availableProcessors() / 2), 1),

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
    val maxWriteAheadLogFileSize: BinarySize = 128.MiB,

    /**
     * The sync mode for files.
     *
     * This mode determines how the files produced by the store are synced to disk.
     * Please refer to the individual enum values for details.
     *
     * This setting will only take effect if disk-based persistence is used.
     */
    val fileSyncMode: FileSyncMode = FileSyncMode.CHANNEL_DATASYNC,

    /**
     * The minimum number of Write-Ahead-Log (WAL) files to keep.
     *
     * If the number of WAL files is less than or equal to this setting,
     * WAL shortening will be skipped.
     */
    val minNumberOfWriteAheadLogFiles: Int = 3,

    /**
     * Determines if ChronoStore should attempt to create a checkpoint during store shutdown.
     *
     * Checkpoints are taken regularly (see: [checkpointCron]) and speed up the restart of the
     * store. By taking a checkpoint during shutdown, we guarantee that a recent checkpoint is
     * available during restart.
     *
     * Disable this option if the time-to-shutdown is more important than the time-to-restart.
     */
    val checkpointOnShutdown: Boolean = true,

    /**
     * The maximum size of the buffer when replaying the write ahead log (WAL).
     *
     * A larger buffer results in faster WAL replay at startup.
     *
     * Please note that this is a **soft** limit. If a single key-value pair is very large,
     * this limit may be temporarily exceeded.
     */
    val walBufferSize: BinarySize = 128.MiB,
) {

    init {
        require(this.maxWriteAheadLogFileSize.bytes > 0) { "Cannot use a negative value for 'maxWriteAheadLogFileSize'!" }
        require(this.checkpointCron?.isValid() ?: true) { "The cron expression for 'writeAheadLogCompactionCron' is invalid: ${this.checkpointCron}" }
    }

    fun createVirtualFileSystemConfiguration(): DiskBasedVirtualFileSystemSettings {
        return DiskBasedVirtualFileSystemSettings(
            fileSyncMode = this.fileSyncMode
        )
    }

}