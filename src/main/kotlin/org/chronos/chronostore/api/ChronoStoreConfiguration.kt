package org.chronos.chronostore.api

import org.chronos.chronostore.io.fileaccess.FileChannelDriver
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.io.format.datablock.BlockReadMode
import java.util.concurrent.TimeUnit

class ChronoStoreConfiguration {

    var maxWriterThreads: Int = 5

    var blockReadMode: BlockReadMode = BlockReadMode.IN_MEMORY_EAGER

    /** When writing new files: the compression algorithm to use. All old files will remain readable if this setting is changed.*/
    var compressionAlgorithm: CompressionAlgorithm = CompressionAlgorithm.SNAPPY

    /** When writing new files: the maximum (uncompressed) size of a single data block.*/
    var maxBlockSizeInBytes: Int = 1024 * 1024 * 64

    /** When writing new files: every n-th key will be part of the block-internal index.*/
    var indexRate: Int = 100

    var randomFileAccessDriverFactory: RandomFileAccessDriverFactory = FileChannelDriver.Factory

    var mergeStrategy: MergeStrategy = MergeStrategy.DEFAULT

    var mergeIntervalMillis: Long = TimeUnit.MINUTES.toMillis(10)

    var maxInMemoryTreeSizeInBytes: Long = 1024 * 1024 * 64

    /**
     * The time of day at which the Write-Ahead-Log file should be compacted.
     *
     * WAL compaction happens once per day, at the specified time-of-day.
     *
     * Use `null` to disable WAL compaction. **WARNING:** disabling WAL compaction
     * can lead to very large WAL files, slow database restart/recovery and increased
     * disk footprint!
     */
    // TODO[Feature]: use cron expression instead
    var writeAheadLogCompactionTimeOfDay: TimeOfDay? = TimeOfDay.parse("00:00")

    /**
     * The time of day at which old store files should be garbage collected.
     *
     * This only affects on-disk files and has no impact on in-memory data structures or caches.
     * This process happens once per day, at the specified time-of-day.
     *
     * Use `null` to disable garbage collection. **WARNING:** disabling garbage compaction
     * can lead to a lot of unused files and increased disk footprint!
     */
    // TODO[Feature]: use cron expression instead
    var garbageCollectionTimeOfDay: TimeOfDay? = TimeOfDay.parse("00:00")

    /**
     * The maximum size of the block cache to use, in bytes.
     *
     * By default, 50% of the JVM heap space is used.
     */
    var blockCacheSizeInBytes: Long = Runtime.getRuntime().maxMemory() / 2

}