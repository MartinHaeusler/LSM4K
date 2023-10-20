package org.chronos.chronostore.api

import org.chronos.chronostore.io.fileaccess.FileChannelDriver
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.util.unit.BinarySize
import org.chronos.chronostore.util.unit.Bytes
import org.chronos.chronostore.util.unit.MiB
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

class ChronoStoreConfiguration {

    var maxWriterThreads: Int = 5

    /** When writing new files: the compression algorithm to use. All old files will remain readable if this setting is changed.*/
    var compressionAlgorithm: CompressionAlgorithm = CompressionAlgorithm.SNAPPY

    /** When writing new files: the maximum (uncompressed) size of a single data block.*/
    var maxBlockSize: BinarySize = 64.MiB

    var randomFileAccessDriverFactory: RandomFileAccessDriverFactory = FileChannelDriver.Factory

    var mergeStrategy: MergeStrategy = MergeStrategy.DEFAULT

    var mergeInterval: Duration? = 10.minutes

    /** The maximum size of the in-memory trees. If this value is exceeded by an insert, the insert is blocked (stalled) until flush tasks have freed memory. */
    var maxForestSize: BinarySize = 250.MiB

    /** This fraction of [maxForestSize] determines when we start flushing to disk. Must be greater than zero and less than 1.0. */
    var forestFlushThreshold: Double = 0.33

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
    var blockCacheSize: BinarySize? = (Runtime.getRuntime().maxMemory() / 4).toInt().Bytes

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
    var fileHeaderCacheSize: BinarySize? = (Runtime.getRuntime().maxMemory() / 100).toInt().Bytes

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
    var maxWriteAheadLogFileSize: BinarySize = 128.MiB


    init {
        require(this.maxWriteAheadLogFileSize.bytes > 0) {
            "Cannot use a negative value for 'maxWriteAheadLogFileSize'!"
        }
    }

}