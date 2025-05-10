package org.chronos.chronostore.util.report

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.chronos.chronostore.api.compaction.CompactionStrategy
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.util.*
import java.util.*

/**
 * A read-only report over the entire database.
 */
class ChronoStoreReport(
    /** The root directory path of this ChronoStore instance. */
    val rootPath: String,
    /** Provides information on the individual stores. */
    val storeReports: Map<StoreId, StoreReport>,
    /** Provides information on the state of the Write-Ahead-Log. */
    val walReport: WalReport,
    /** Provides information on past and ongoing transactions. */
    val transactionReport: TransactionReport,
    /** The highest committed [TSN] across all stores. `null` if no commit has occurred yet. */
    val currentTSN: TSN?,
    /** The highest [TSN] which has been fully written to LSM files (not Write-Ahead-Log files) across all stores. `null` if no commit has been fully persisted yet. */
    val maxPersistedTSN: TSN?,
)

/**
 * Provides information about the current state of a single store.
 */
class StoreReport(
    /** The unique ID of this store. */
    val storeId: StoreId,
    /** The path of this store on disk. */
    val path: String,
    /** The compaction strategy used by this store. */
    val compactionStrategy: CompactionStrategy,
    /** The compression algorithm used by all new files added to this store. */
    val compressionAlgorithm: CompressionAlgorithm,
    /** Contains information on the individual levels/tiers. */
    val layers: Map<LevelOrTierIndex, LayerReport>,
    /** Contains information about the current state of this store's memtable. */
    val memTable: MemtableReport,
    /** The highest [TSN] which has ever committed any data into this store so far. `null` if no commit has occurred on this store yet.  */
    val currentTSN: TSN?,
    /** The highest [TSN] which has been fully written to LSM files (not Write-Ahead-Log files). `null` if no commit on this store has been fully persisted yet.*/
    val maxPersistedTSN: TSN?,
)

/**
 * Provides information on the current state of the Write-Ahead-Log.
 */
class WalReport(
    /** The on-disk write-ahead-log files. */
    val files: List<WalFileReport>,
)

/**
 * The current state of a single level/tier in a store.
 */
class LayerReport(
    /**
     * The 0-based index of the level/tier.
     *
     * Index 0 is where new data comes in (produced by flush tasks).
     * Higher indices contain progressively older data.
     */
    val layerIndex: LevelOrTierIndex,

    /** The state of the individual files that belong to this level/tier. */
    val files: List<StoreFileReport>,
)

/**
 * The current state of the memtable for a single store.
 */
class MemtableReport(
    /** The size of the memtable. */
    val sizeInBytes: Long,
    /** The minimum key in the memtable as a hexadecimal string. If `null`, the memtable is currently empty. */
    val minKey: Hex?,
    /** The maximum key in the memtable as a hexadecimal string. If `null`, the memtable is currently empty. */
    val maxKey: Hex?,
    /** The minimum [TSN] in the memtable. If `null`, the memtable is currently empty. */
    val minTsn: TSN?,
    /** The maximum [TSN] in the memtable. If `null`, the memtable is currently empty. */
    val maxTsn: TSN?,
    /** The minimum key-and-TSN in the memtable. If `null`, the memtable is currently empty. */
    val minKeyAndTSN: HexKeyAndTSN?,
    /** The maximum key-and-TSN in the memtable. If `null`, the memtable is currently empty. */
    val maxKeyAndTSN: HexKeyAndTSN?,
)

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(
        value = WalFileReport::class,
        name = "walFile",
    ),
    JsonSubTypes.Type(
        value = StoreFileReport::class,
        name = "storeFile",
    ),
)
sealed interface FileReport {
    /** The path of the file. */
    val path: String

    /** The name of the file. */
    val name: String

    /** The on-disk size of the file. */
    val sizeInBytes: Long

}

/**
 * Provides information on a single Write-Ahead-Log file.
 */
class WalFileReport(
    override val path: String,
    override val name: String,
    override val sizeInBytes: Long,
) : FileReport

/**
 * Provides information on a single LSM file.
 */
class StoreFileReport(
    override val path: String,
    override val name: String,
    override val sizeInBytes: Long,

    /** The semantic format version of this file. */
    val formatVersion: String,
    /** The index of this file (corresponds to the file name). */
    val fileIndex: FileIndex,
    /** The unique identifier of this file. */
    val uuid: UUID,
    /** The total number of blocks in this file. */
    val numberOfBlocks: Int,
    /** The number of compactions applied to the data in this file. */
    val numberOfCompactions: Long,
    /** The compression algorithm used by this file. */
    val compressionAlgorithm: CompressionAlgorithm,
    /** The wall-clock unix timestamp when this file was created (in milliseconds). */
    val createdAt: Timestamp,
    /** The total number of key-value entries in this file. */
    val totalEntries: Long,
    /** The number of key-value entries in this file which belong to the latest version of the data.*/
    val headEntries: Long,
    /** The number of key-value entries in this file which belong to historic versions of the data. */
    val historyEntries: Long,
    /** The minimum key in the file as a hexadecimal string. `null` if the file is empty. */
    val minKey: Hex?,
    /** The maximum key in the file as a hexadecimal string. `null` if the file is empty. */
    val maxKey: Hex?,
    /** The minimum [TSN] in the file. `null` if the file is empty. */
    val minTsn: TSN?,
    /** The maximum [TSN] in the file. `null` if the file is empty. */
    val maxTsn: TSN?,
    /** The minimum key-and-TSN in the file. `null` if the file is empty. */
    val minKeyAndTSN: HexKeyAndTSN?,
    /** The maximum key-and-TSN in the file. `null` if the file is empty. */
    val maxKeyAndTSN: HexKeyAndTSN?,
    /** Is the file awaiting garbage collection? */
    val isGarbage: Boolean,
) : FileReport

class TransactionReport(
    /** Indicates many transactions have been processed since the last startup. */
    val transactionsProcessed: Long,
    /** Indicates the number of currently open transactions. */
    val openTransactions: Int,
    /** Indicates the runtime of the oldest (still running) transaction in milliseconds. */
    val longestOpenTransactionDurationInMilliseconds: Long?,
)

data class HexKeyAndTSN(
    /** The key bytes, in a hexadecimal format. */
    val key: Hex,
    /** The [TSN] of this element. */
    val tsn: TSN,
)