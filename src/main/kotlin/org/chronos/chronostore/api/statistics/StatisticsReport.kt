package org.chronos.chronostore.api.statistics

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.TransactionMode
import org.chronos.chronostore.api.statistics.CursorStatisticsReport.Companion.sum
import org.chronos.chronostore.api.statistics.TransactionStatisticsReport.Companion.sum
import org.chronos.chronostore.io.format.cursor.ChronoStoreFileCursor
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.cursor.OverlayCursor
import org.chronos.chronostore.util.cursor.VersioningCursor
import org.chronos.chronostore.util.unit.BinarySize.Companion.Bytes
import java.util.*
import kotlin.time.Duration.Companion.milliseconds

/**
 * Immutable snapshot of the statistics of a [ChronoStore].
 */
data class StatisticsReport(
    /** When did the tracking of events start? */
    val trackingStartTimestamp: Timestamp,

    /** When did the tracking of events end? */
    val trackingEndTimestamp: Timestamp,

    /** Cursor statistics by cursor class name. */
    val cursorStatistics: Map<String, CursorStatisticsReport>,

    /** Transaction statistics by [TransactionMode]. */
    val transactionStatistics: Map<TransactionMode, TransactionStatisticsReport>,

    /** Statistics about the block cache. */
    val blockCacheStatistics: CacheStatisticsReport,

    /** Statistics about the file header cache. */
    val fileHeaderCacheStatistics: CacheStatisticsReport,

    /** How many times did we load a block from raw bytes? */
    val blockLoads: Long,

    /** How much time (in milliseconds) did we spend loading blocks? */
    val blockLoadTime: Long,

    /** How long have writer threads been stalled because of full in-memory trees? */
    val totalWriteStallTimeMillis: Long,

    /** How many times have writer threads been stalled because of full in-memory trees? */
    val writeStallEvents: Long,

    /** How many flush tasks have been executed? */
    val flushTaskExecutions: Long,

    /** How many bytes have been written by the flush tasks in total? */
    val flushTaskWrittenBytes: Long,

    /** How many key-value entries have been written by the flush tasks in total? */
    val flushTaskWrittenEntries: Long,

    /** How much time has been spent in total in flush tasks (possibly concurrently)? */
    val flushTaskTotalTime: Long,

    /** How many times did we compress raw bytes? */
    val compressionInvocations: Long,

    /** How many input bytes did we receive in total for compression? */
    val compressionInputBytes: Long,

    /** How many output bytes did we deliver in total for compression? */
    val compressionOutputBytes: Long,

    /** How many times did we uncompress bytes? */
    val decompressionInvocations: Long,

    /** How many input bytes did we receive in total for decompression? */
    val decompressionInputBytes: Long,

    /** How many output bytes did we deliver in total for decompression? */
    val decompressionOutputBytes: Long,
) {

    val allCursorStatistics: CursorStatisticsReport = this.cursorStatistics.values.sum()

    val fileCursorStatistics: CursorStatisticsReport
        get() = this.getCursorStatistics(ChronoStoreFileCursor.CURSOR_NAME)

    val overlayCursorStatistics: CursorStatisticsReport
        get() = this.getCursorStatistics(OverlayCursor.CURSOR_NAME)

    val versioningCursorStatistics: CursorStatisticsReport
        get() = this.getCursorStatistics(VersioningCursor.CURSOR_NAME)

    val allTransactionStatistics: TransactionStatisticsReport = this.transactionStatistics.values.sum()

    val readOnlyTransactionStatistics: TransactionStatisticsReport
        get() = this.getTransactionStatistics(TransactionMode.READ_ONLY)

    val readWriteTransactionStatistics: TransactionStatisticsReport
        get() = this.getTransactionStatistics(TransactionMode.READ_WRITE)

    val exclusiveTransactionStatistics: TransactionStatisticsReport
        get() = this.getTransactionStatistics(TransactionMode.EXCLUSIVE)

    fun getCursorStatistics(cursorType: String): CursorStatisticsReport {
        return this.cursorStatistics[cursorType] ?: CursorStatisticsReport.empty(cursorType)
    }

    fun getTransactionStatistics(mode: TransactionMode): TransactionStatisticsReport {
        return this.transactionStatistics[mode] ?: TransactionStatisticsReport.empty(mode)
    }

    fun prettyPrint(): String {
        return """ChronoStore Statistics
            | Tracking Start: ${Date(this.trackingStartTimestamp)} (Timestamp: ${this.trackingStartTimestamp})
            | Tracking End: ${Date(this.trackingEndTimestamp)} (Timestamp: ${this.trackingEndTimestamp})
            | Tracking Duration: ${(this.trackingStartTimestamp - this.trackingEndTimestamp).milliseconds}
            | 
            | All Cursors: ${this.allCursorStatistics.cursorsOpened}
            |     Operations: ${this.allCursorStatistics.operations}
            |         Moves: ${this.allCursorStatistics.moves}
            |             Next: ${this.allCursorStatistics.cursorOperationsNext}
            |             Prev: ${this.allCursorStatistics.cursorOperationsPrevious}
            |         Jumps: ${this.allCursorStatistics.jumps}
            |            First: ${this.allCursorStatistics.cursorOperationsFirst}
            |             Last: ${this.allCursorStatistics.cursorOperationsLast}
            |         Seeks: ${this.allCursorStatistics.seeks}
            |           Higher: ${this.allCursorStatistics.cursorOperationsSeekExactlyOrNext}
            |            Lower: ${this.allCursorStatistics.cursorOperationsSeekExactlyOrPrevious}
            | File Cursors: ${this.fileCursorStatistics.cursorsOpened}
            |     Operations: ${this.fileCursorStatistics.operations}
            |         Moves: ${this.fileCursorStatistics.moves}
            |             Next: ${this.fileCursorStatistics.cursorOperationsNext}
            |             Prev: ${this.fileCursorStatistics.cursorOperationsPrevious}
            |         Jumps: ${this.fileCursorStatistics.jumps}
            |            First: ${this.fileCursorStatistics.cursorOperationsFirst}
            |             Last: ${this.fileCursorStatistics.cursorOperationsLast}
            |         Random: ${this.fileCursorStatistics.seeks}
            |           Higher: ${this.fileCursorStatistics.cursorOperationsSeekExactlyOrNext}
            |            Lower: ${this.fileCursorStatistics.cursorOperationsSeekExactlyOrPrevious}
            | Overlay Cursors: ${this.overlayCursorStatistics.cursorsOpened}
            |     Operations: ${this.overlayCursorStatistics.operations}
            |         Moves: ${this.overlayCursorStatistics.moves}
            |             Next: ${this.overlayCursorStatistics.cursorOperationsNext}
            |             Prev: ${this.overlayCursorStatistics.cursorOperationsPrevious}
            |         Jumps: ${this.overlayCursorStatistics.jumps}
            |            First: ${this.overlayCursorStatistics.cursorOperationsFirst}
            |             Last: ${this.overlayCursorStatistics.cursorOperationsLast}
            |         Seeks: ${this.overlayCursorStatistics.seeks}
            |           Higher: ${this.overlayCursorStatistics.cursorOperationsSeekExactlyOrNext}
            |            Lower: ${this.overlayCursorStatistics.cursorOperationsSeekExactlyOrPrevious}
            | Versioning Cursors: ${this.versioningCursorStatistics.cursorsOpened}
            |     Operations: ${this.versioningCursorStatistics.operations}
            |         Moves: ${this.versioningCursorStatistics.moves}
            |             Next: ${this.versioningCursorStatistics.cursorOperationsNext}
            |             Prev: ${this.versioningCursorStatistics.cursorOperationsSeekExactlyOrPrevious}
            |         Jumps: ${this.versioningCursorStatistics.jumps}
            |            First: ${this.versioningCursorStatistics.cursorOperationsFirst}
            |             Last: ${this.versioningCursorStatistics.cursorOperationsLast}
            |         Seeks: ${this.versioningCursorStatistics.seeks}
            |           Higher: ${this.versioningCursorStatistics.cursorOperationsSeekExactlyOrNext}
            |            Lower: ${this.versioningCursorStatistics.cursorOperationsSeekExactlyOrPrevious}
            | Caches:
            |     Block Cache:
            |            Requests: ${this.blockCacheStatistics.requests}
            |                Hits: ${this.blockCacheStatistics.hits}
            |              Misses: ${this.blockCacheStatistics.misses}
            |            Hit Rate: ${"%.2f".format(this.blockCacheStatistics.hitRate * 100)}%
            |           Evictions: ${this.blockCacheStatistics.evictions}
            |     Header Cache:
            |            Requests: ${this.fileHeaderCacheStatistics.requests}
            |                Hits: ${this.fileHeaderCacheStatistics.hits}
            |              Misses: ${this.fileHeaderCacheStatistics.misses}
            |            Hit Rate: ${"%.2f".format(this.fileHeaderCacheStatistics.hitRate * 100)}%
            |           Evictions: ${this.fileHeaderCacheStatistics.evictions}
            | Block Management:
            |        Block Loads: ${this.blockLoads}
            |    Block Load Time: ${this.blockLoadTime}ms
            | Flush Tasks:
            |    Flushes Executed: ${this.flushTaskExecutions}
            |       Bytes Written: ${this.flushTaskWrittenBytes.Bytes.toHumanReadableString()}
            |     Entries Written: ${this.flushTaskWrittenEntries}
            |          Total Time: ${this.flushTaskTotalTime}ms
            | Write Stalling:
            |    Total Stall Time: ${this.totalWriteStallTimeMillis}ms
            |        Stall Events: ${this.writeStallEvents}
            | Transactions:
            |         Opened: ${this.allTransactionStatistics.openedTransactions}
            |            Read-Only: ${this.readOnlyTransactionStatistics.openedTransactions}
            |           Read-Write: ${this.readWriteTransactionStatistics.openedTransactions}
            |            Exclusive: ${this.exclusiveTransactionStatistics.openedTransactions}
            |      Committed: ${this.allTransactionStatistics.committedTransactions}
            |            Read-Only: ${this.readOnlyTransactionStatistics.committedTransactions}
            |           Read-Write: ${this.readWriteTransactionStatistics.committedTransactions}
            |            Exclusive: ${this.exclusiveTransactionStatistics.committedTransactions}
            |    Rolled back: ${this.allTransactionStatistics.rollbackTransactions}
            |            Read-Only: ${this.readOnlyTransactionStatistics.rollbackTransactions}
            |           Read-Write: ${this.readWriteTransactionStatistics.rollbackTransactions}
            |            Exclusive: ${this.exclusiveTransactionStatistics.rollbackTransactions}
            |       Dangling: ${this.allTransactionStatistics.danglingTransactions}
            |            Read-Only: ${this.readOnlyTransactionStatistics.danglingTransactions}
            |           Read-Write: ${this.readWriteTransactionStatistics.danglingTransactions}
            |            Exclusive: ${this.exclusiveTransactionStatistics.danglingTransactions}
            | Compression & Decompression:
            |   Compression:
            |    Invocations: ${this.compressionInvocations}
            |       Bytes In: ${this.compressionInputBytes.Bytes.toHumanReadableString()}
            |      Bytes Out: ${this.compressionOutputBytes.Bytes.toHumanReadableString()}
            |   Decompression:
            |    Invocations: ${this.decompressionInvocations}
            |       Bytes In: ${this.decompressionInputBytes.Bytes.toHumanReadableString()}
            |      Bytes Out: ${this.decompressionOutputBytes.Bytes.toHumanReadableString()}
        """.trimMargin()
    }

    override fun toString(): String {
        return "StatisticsReport[${Date(this.trackingStartTimestamp)}..${Date(this.trackingEndTimestamp)}]"
    }
}