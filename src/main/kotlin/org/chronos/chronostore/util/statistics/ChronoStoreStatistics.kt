package org.chronos.chronostore.util.statistics

import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.cursor.VersioningCursor
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics.Companion.TRACKING_START
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics.Companion.reset
import org.chronos.chronostore.util.unit.BinarySize.Companion.Bytes
import java.util.*
import java.util.concurrent.atomic.AtomicLong

class ChronoStoreStatistics(
    /** The timestamp at which the tracking of statistics has started. */
    val trackingStartedAt: Timestamp,
    /** Statistics on actual low-level file cursors.*/
    val fileCursorStatistics: CursorStatistics,
    /** Statistics on cursors which combine two cursors by overlaying them. */
    val overlayCursorStatistics: CursorStatistics,
    /** Statistics on cursors which perform version resolution.*/
    val versioningCursorStatistics: CursorStatistics,
    /** Statistics on the block cache. */
    val blockCache: CacheStatistics,
    /** How many times did we load a block from raw bytes? */
    val blockLoads: Long,
    /** How much time (in milliseconds) did we spend loading blocks? */
    val blockLoadTime: Long,
    /** Statistics on the file header cache.  */
    val fileHeaderCache: CacheStatistics,
    /** Statistics on the usage of compression. */
    val compressionStatistics: CompressionStatistics,
    /** How long have writer threads been stalled because of full in-memory trees? */
    val totalWriteStallTimeMillis: Long,
    /** How many times have writer threads been stalled because of full in-memory trees? */
    val writeStallEvents: Long,
    /** The total number of transactions. */
    val transactionsOpened: Long,
    /** How many transactions have been committed? */
    val transactionsCommitted: Long,
    /** How many transactions have been rolled back? */
    val transactionsRolledBack: Long,
    /** How many transactions have been opened but then left open and forgotten? */
    val transactionsDangling: Long,
    /** How many flush tasks have been executed? */
    val flushTaskExecutions: Long,
    /** How many bytes have been written by the flush tasks in total? */
    val flushTaskWrittenBytes: Long,
    /** How many key-value entries have been written by the flush tasks in total? */
    val flushTaskWrittenEntries: Long,
    /** How much time has been spent in total in flush tasks (possibly concurrently)? */
    val flushTaskTotalTime: Long,
) {

    companion object {

        /** When did the tracking of events start? Will be reassigned when [reset] is called. */
        val TRACKING_START = AtomicLong(System.currentTimeMillis())

        /** How many file cursors have been opened so far? */
        val FILE_CURSORS = AtomicLong(0L)

        /** How many times has "cursor.seekExactlyOrNext()" been called on a ChronoStoreFileCursor? */
        val FILE_CURSOR_EXACTLY_OR_NEXT_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.seekExactlyOrPrevious" been called on a ChronoStoreFileCursor? */
        val FILE_CURSOR_EXACTLY_OR_PREVIOUS_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.first()" been called on a ChronoStoreFileCursor? */
        val FILE_CURSOR_FIRST_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.last()" been called on a ChronoStoreFileCursor? */
        val FILE_CURSOR_LAST_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.next()" been called on a ChronoStoreFileCursor? */
        val FILE_CURSOR_NEXT_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.previous()" been called on a ChronoStoreFileCursor? */
        val FILE_CURSOR_PREVIOUS_SEEKS = AtomicLong(0L)

        /** How many [OverlayCursor]s have been opened so far? */
        val OVERLAY_CURSORS = AtomicLong(0L)

        /** How many times has "cursor.seekExactlyOrNext()" been called on an [OverlayCursor]? */
        val OVERLAY_CURSOR_EXACTLY_OR_NEXT_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.seekExactlyOrPrevious" been called on a [OverlayCursor]? */
        val OVERLAY_CURSOR_EXACTLY_OR_PREVIOUS_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.first()" been called on a [OverlayCursor]? */
        val OVERLAY_CURSOR_FIRST_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.last()" been called on a [OverlayCursor]? */
        val OVERLAY_CURSOR_LAST_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.next()" been called on a [OverlayCursor]? */
        val OVERLAY_CURSOR_NEXT_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.previous()" been called on a [OverlayCursor]? */
        val OVERLAY_CURSOR_PREVIOUS_SEEKS = AtomicLong(0L)

        /** How many [VersioningCursor]s have been opened so far? */
        val VERSIONING_CURSORS = AtomicLong(0L)

        /** How many times has "cursor.seekExactlyOrNext()" been called on an [VersioningCursor]? */
        val VERSIONING_CURSOR_EXACTLY_OR_NEXT_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.seekExactlyOrPrevious" been called on a [VersioningCursor]? */
        val VERSIONING_CURSOR_EXACTLY_OR_PREVIOUS_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.first()" been called on a [VersioningCursor]? */
        val VERSIONING_CURSOR_FIRST_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.last()" been called on a [VersioningCursor]? */
        val VERSIONING_CURSOR_LAST_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.next()" been called on a [VersioningCursor]? */
        val VERSIONING_CURSOR_NEXT_SEEKS = AtomicLong(0L)

        /** How many times has "cursor.previous()" been called on a [VersioningCursor]? */
        val VERSIONING_CURSOR_PREVIOUS_SEEKS = AtomicLong(0L)

        /** How many blocks have been evicted from the cache? */
        val BLOCK_CACHE_REQUESTS = AtomicLong(0L)

        /** How many cache misses have occurred in the block cache? */
        val BLOCK_CACHE_MISSES = AtomicLong(0L)

        /** How many cache evictions have occurred in the block cache? */
        val BLOCK_CACHE_EVICTIONS = AtomicLong(0L)

        /** How many times did we load a block from raw bytes? */
        val BLOCK_LOADS = AtomicLong(0L)

        /** How much time (in milliseconds) did we spend loading blocks? */
        val BLOCK_LOAD_TIME = AtomicLong(0L)

        /** How many file headers have been evicted from the cache?  */
        val FILE_HEADER_CACHE_EVICTIONS = AtomicLong(0L)

        /** How many cache hits have occurred in the file header cache? */
        val FILE_HEADER_CACHE_REQUESTS = AtomicLong(0L)

        /** How many cache misses have occurred in the file header cache? */
        val FILE_HEADER_CACHE_MISSES = AtomicLong(0L)

        /** How long have writer threads been stalled because of full in-memory trees? */
        val TOTAL_WRITE_STALL_TIME_MILLIS = AtomicLong(0L)

        /** How many times have writer threads been stalled because of full in-memory trees? */
        val WRITE_STALL_EVENTS = AtomicLong(0L)

        /** How many transactions have been opened? */
        val TRANSACTIONS = AtomicLong(0L)

        /** How many transactions have been committed? */
        val TRANSACTION_COMMITS = AtomicLong(0L)

        /** How many transactions have been rolled back? */
        val TRANSACTION_ROLLBACKS = AtomicLong(0L)

        /** How many transactions have been left open and forgotten? */
        val TRANSACTION_DANGLING = AtomicLong(0L)

        /** How many flush tasks have been executed? */
        val FLUSH_TASK_EXECUTIONS = AtomicLong(0L)

        /** How many bytes have been written by the flush tasks in total? */
        val FLUSH_TASK_WRITTEN_BYTES = AtomicLong(0L)

        /** How many key-value entries have been written by the flush tasks in total? */
        val FLUSH_TASK_WRITTEN_ENTRIES = AtomicLong(0L)

        /** How much time has been spent in total in flush tasks (possibly concurrently)? */
        val FLUSH_TASK_TOTAL_TIME = AtomicLong(0L)

        /** How many times did we compress raw bytes? */
        val COMPRESSION_INVOCATIONS = AtomicLong(0L)

        /** How many input bytes did we receive in total for compression? */
        val COMPRESSION_INPUT_BYTES = AtomicLong(0L)

        /** How many output bytes did we deliver in total for compression? */
        val COMPRESSION_OUTPUT_BYTES = AtomicLong(0L)

        /** How many times did we uncompress bytes? */
        val DECOMPRESSION_INVOCATIONS = AtomicLong(0L)

        /** How many input bytes did we receive in total for decompression? */
        val DECOMPRESSION_INPUT_BYTES = AtomicLong(0L)

        /** How many output bytes did we deliver in total for decompression? */
        val DECOMPRESSION_OUTPUT_BYTES = AtomicLong(0L)

        /**
         * Retrieves an immutable snapshot of all statistics at the current point in time.
         */
        fun snapshot(): ChronoStoreStatistics {
            return ChronoStoreStatistics(
                trackingStartedAt = TRACKING_START.get(),
                CursorStatistics(
                    groupName = "File Cursors",
                    cursors = FILE_CURSORS.get(),
                    cursorExactlyOrNextSeeks = FILE_CURSOR_EXACTLY_OR_NEXT_SEEKS.get(),
                    cursorExactlyOrPreviousSeeks = FILE_CURSOR_EXACTLY_OR_PREVIOUS_SEEKS.get(),
                    cursorFirstSeeks = FILE_CURSOR_FIRST_SEEKS.get(),
                    cursorLastSeeks = FILE_CURSOR_LAST_SEEKS.get(),
                    cursorNextSeeks = FILE_CURSOR_NEXT_SEEKS.get(),
                    cursorPreviousSeeks = FILE_CURSOR_PREVIOUS_SEEKS.get(),
                ),
                CursorStatistics(
                    groupName = "Overlay Cursors",
                    cursors = OVERLAY_CURSORS.get(),
                    cursorExactlyOrNextSeeks = OVERLAY_CURSOR_EXACTLY_OR_NEXT_SEEKS.get(),
                    cursorExactlyOrPreviousSeeks = OVERLAY_CURSOR_EXACTLY_OR_PREVIOUS_SEEKS.get(),
                    cursorFirstSeeks = OVERLAY_CURSOR_FIRST_SEEKS.get(),
                    cursorLastSeeks = OVERLAY_CURSOR_LAST_SEEKS.get(),
                    cursorNextSeeks = OVERLAY_CURSOR_NEXT_SEEKS.get(),
                    cursorPreviousSeeks = OVERLAY_CURSOR_PREVIOUS_SEEKS.get(),
                ),
                CursorStatistics(
                    groupName = "Versioning Cursors",
                    cursors = VERSIONING_CURSORS.get(),
                    cursorExactlyOrNextSeeks = VERSIONING_CURSOR_EXACTLY_OR_NEXT_SEEKS.get(),
                    cursorExactlyOrPreviousSeeks = VERSIONING_CURSOR_EXACTLY_OR_PREVIOUS_SEEKS.get(),
                    cursorFirstSeeks = VERSIONING_CURSOR_FIRST_SEEKS.get(),
                    cursorLastSeeks = VERSIONING_CURSOR_LAST_SEEKS.get(),
                    cursorNextSeeks = VERSIONING_CURSOR_NEXT_SEEKS.get(),
                    cursorPreviousSeeks = VERSIONING_CURSOR_PREVIOUS_SEEKS.get(),
                ),

                blockCache = CacheStatistics(
                    cacheName = "Block Cache",
                    evictions = BLOCK_CACHE_EVICTIONS.get(),
                    misses = BLOCK_CACHE_MISSES.get(),
                    requests = BLOCK_CACHE_REQUESTS.get(),
                ),

                blockLoads = BLOCK_LOADS.get(),
                blockLoadTime = BLOCK_LOAD_TIME.get(),

                fileHeaderCache = CacheStatistics(
                    cacheName = "File Header Cache",
                    evictions = FILE_HEADER_CACHE_EVICTIONS.get(),
                    misses = FILE_HEADER_CACHE_MISSES.get(),
                    requests = FILE_HEADER_CACHE_REQUESTS.get(),
                ),

                compressionStatistics = CompressionStatistics(
                    compressionInvocations = COMPRESSION_INVOCATIONS.get(),
                    compressionInputBytes = COMPRESSION_INPUT_BYTES.get(),
                    compressionOutputBytes = COMPRESSION_OUTPUT_BYTES.get(),
                    decompressionInvocations = DECOMPRESSION_INVOCATIONS.get(),
                    decompressionInputBytes = DECOMPRESSION_INPUT_BYTES.get(),
                    decompressionOutputBytes = DECOMPRESSION_OUTPUT_BYTES.get(),
                ),

                totalWriteStallTimeMillis = TOTAL_WRITE_STALL_TIME_MILLIS.get(),
                writeStallEvents = WRITE_STALL_EVENTS.get(),
                transactionsOpened = TRANSACTIONS.get(),
                transactionsCommitted = TRANSACTION_COMMITS.get(),
                transactionsRolledBack = TRANSACTION_ROLLBACKS.get(),
                transactionsDangling = TRANSACTION_DANGLING.get(),
                flushTaskExecutions = FLUSH_TASK_EXECUTIONS.get(),
                flushTaskWrittenBytes = FLUSH_TASK_WRITTEN_BYTES.get(),
                flushTaskWrittenEntries = FLUSH_TASK_WRITTEN_ENTRIES.get(),
                flushTaskTotalTime = FLUSH_TASK_TOTAL_TIME.get(),
            )
        }

        /**
         * Resets the statistics.
         *
         * The [TRACKING_START] will be set to [System.currentTimeMillis], and all
         * other statistics will be set to zero.
         */
        fun reset() {
            TRACKING_START.set(System.currentTimeMillis())

            FILE_CURSORS.set(0L)
            FILE_CURSOR_EXACTLY_OR_NEXT_SEEKS.set(0L)
            FILE_CURSOR_EXACTLY_OR_PREVIOUS_SEEKS.set(0L)
            FILE_CURSOR_FIRST_SEEKS.set(0L)
            FILE_CURSOR_LAST_SEEKS.set(0L)
            FILE_CURSOR_NEXT_SEEKS.set(0L)
            FILE_CURSOR_PREVIOUS_SEEKS.set(0L)

            OVERLAY_CURSORS.set(0L)
            OVERLAY_CURSOR_EXACTLY_OR_NEXT_SEEKS.set(0L)
            OVERLAY_CURSOR_EXACTLY_OR_PREVIOUS_SEEKS.set(0L)
            OVERLAY_CURSOR_FIRST_SEEKS.set(0L)
            OVERLAY_CURSOR_LAST_SEEKS.set(0L)
            OVERLAY_CURSOR_NEXT_SEEKS.set(0L)
            OVERLAY_CURSOR_PREVIOUS_SEEKS.set(0L)

            VERSIONING_CURSORS.set(0L)
            VERSIONING_CURSOR_EXACTLY_OR_NEXT_SEEKS.set(0L)
            VERSIONING_CURSOR_EXACTLY_OR_PREVIOUS_SEEKS.set(0L)
            VERSIONING_CURSOR_FIRST_SEEKS.set(0L)
            VERSIONING_CURSOR_LAST_SEEKS.set(0L)
            VERSIONING_CURSOR_NEXT_SEEKS.set(0L)
            VERSIONING_CURSOR_PREVIOUS_SEEKS.set(0L)

            BLOCK_CACHE_MISSES.set(0L)
            BLOCK_CACHE_EVICTIONS.set(0L)
            BLOCK_CACHE_REQUESTS.set(0L)

            BLOCK_LOADS.set(0L)
            BLOCK_LOAD_TIME.set(0L)

            FILE_HEADER_CACHE_EVICTIONS.set(0L)
            FILE_HEADER_CACHE_REQUESTS.set(0L)
            FILE_HEADER_CACHE_MISSES.set(0L)

            TOTAL_WRITE_STALL_TIME_MILLIS.set(0L)
            WRITE_STALL_EVENTS.set(0L)

            TRANSACTIONS.set(0L)
            TRANSACTION_COMMITS.set(0L)
            TRANSACTION_ROLLBACKS.set(0L)
            TRANSACTION_DANGLING.set(0L)

            FLUSH_TASK_EXECUTIONS.set(0L)
            FLUSH_TASK_TOTAL_TIME.set(0L)
            FLUSH_TASK_WRITTEN_BYTES.set(0L)
            FLUSH_TASK_WRITTEN_ENTRIES.set(0L)

            COMPRESSION_INVOCATIONS.set(0L)
            COMPRESSION_INPUT_BYTES.set(0L)
            COMPRESSION_OUTPUT_BYTES.set(0L)
            DECOMPRESSION_INVOCATIONS.set(0L)
            DECOMPRESSION_INPUT_BYTES.set(0L)
            DECOMPRESSION_OUTPUT_BYTES.set(0L)
        }
    }

    val cursorGroups = listOf(this.fileCursorStatistics, this.overlayCursorStatistics, this.versioningCursorStatistics)

    val cursorGroupNameToStatistics = cursorGroups.associateBy { it.groupName }

    val allCursorStatistics: CursorStatistics = cursorGroups.reduce(CursorStatistics::plus).withName("All Cursors")


    fun prettyPrint(): String {
        return """ChronoStore Statistics
            | Tracking Start: ${Date(this.trackingStartedAt)} (Timestamp: ${this.trackingStartedAt})
            | All Cursors: ${this.allCursorStatistics.cursors}
            |     Seeks: ${this.allCursorStatistics.totalSeeks}
            |         Moves: ${this.allCursorStatistics.totalMoves}
            |             Next: ${this.allCursorStatistics.cursorNextSeeks}
            |             Prev: ${this.allCursorStatistics.cursorPreviousSeeks}
            |         Jumps: ${this.allCursorStatistics.totalJumps}
            |             First: ${this.allCursorStatistics.cursorFirstSeeks}
            |              Last: ${this.allCursorStatistics.cursorLastSeeks}
            |         Random: ${this.allCursorStatistics.totalRandomSeek}
            |             Higher: ${this.allCursorStatistics.cursorExactlyOrNextSeeks}
            |              Lower: ${this.allCursorStatistics.cursorExactlyOrPreviousSeeks}
            | File Cursors: ${this.fileCursorStatistics.cursors}
            |     Seeks: ${this.fileCursorStatistics.totalSeeks}
            |         Moves: ${this.fileCursorStatistics.totalMoves}
            |             Next: ${this.fileCursorStatistics.cursorNextSeeks}
            |             Prev: ${this.fileCursorStatistics.cursorPreviousSeeks}
            |         Jumps: ${this.fileCursorStatistics.totalJumps}
            |             First: ${this.fileCursorStatistics.cursorFirstSeeks}
            |              Last: ${this.fileCursorStatistics.cursorLastSeeks}
            |         Random: ${this.fileCursorStatistics.totalRandomSeek}
            |             Higher: ${this.fileCursorStatistics.cursorExactlyOrNextSeeks}
            |              Lower: ${this.fileCursorStatistics.cursorExactlyOrPreviousSeeks}
            | Overlay Cursors: ${this.overlayCursorStatistics.cursors}
            |     Seeks: ${this.overlayCursorStatistics.totalSeeks}
            |         Moves: ${this.overlayCursorStatistics.totalMoves}
            |             Next: ${this.overlayCursorStatistics.cursorNextSeeks}
            |             Prev: ${this.overlayCursorStatistics.cursorPreviousSeeks}
            |         Jumps: ${this.overlayCursorStatistics.totalJumps}
            |             First: ${this.overlayCursorStatistics.cursorFirstSeeks}
            |              Last: ${this.overlayCursorStatistics.cursorLastSeeks}
            |         Random: ${this.overlayCursorStatistics.totalRandomSeek}
            |             Higher: ${this.overlayCursorStatistics.cursorExactlyOrNextSeeks}
            |              Lower: ${this.overlayCursorStatistics.cursorExactlyOrPreviousSeeks}
            | Versioning Cursors: ${this.versioningCursorStatistics.cursors}
            |     Seeks: ${this.versioningCursorStatistics.totalSeeks}
            |         Moves: ${this.versioningCursorStatistics.totalMoves}
            |             Next: ${this.versioningCursorStatistics.cursorNextSeeks}
            |             Prev: ${this.versioningCursorStatistics.cursorPreviousSeeks}
            |         Jumps: ${this.versioningCursorStatistics.totalJumps}
            |             First: ${this.versioningCursorStatistics.cursorFirstSeeks}
            |              Last: ${this.versioningCursorStatistics.cursorLastSeeks}
            |         Random: ${this.versioningCursorStatistics.totalRandomSeek}
            |             Higher: ${this.versioningCursorStatistics.cursorExactlyOrNextSeeks}
            |              Lower: ${this.versioningCursorStatistics.cursorExactlyOrPreviousSeeks}
            | Caches:
            |     Block Cache:
            |            Requests: ${this.blockCache.requests}
            |                Hits: ${this.blockCache.hits}
            |              Misses: ${this.blockCache.misses}
            |            Hit Rate: ${"%.2f".format(this.blockCache.hitRate * 100)}%
            |           Evictions: ${this.blockCache.evictions}
            |     Header Cache:
            |            Requests: ${this.fileHeaderCache.requests}
            |                Hits: ${this.fileHeaderCache.hits}
            |              Misses: ${this.fileHeaderCache.misses}
            |            Hit Rate: ${"%.2f".format(this.fileHeaderCache.hitRate * 100)}%
            |           Evictions: ${this.fileHeaderCache.evictions}
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
            |         Opened: ${this.transactionsOpened}
            |      Committed: ${this.transactionsCommitted}
            |    Rolled back: ${this.transactionsRolledBack}
            |       Dangling: ${this.transactionsDangling}
            | Compression:
            |   Compression:
            |    Invocations: ${this.compressionStatistics.compressionInvocations}
            |       Bytes In: ${this.compressionStatistics.compressionInputBytes.Bytes.toHumanReadableString()}
            |      Bytes Out: ${this.compressionStatistics.compressionOutputBytes.Bytes.toHumanReadableString()}
            |   Decompression:
            |    Invocations: ${this.compressionStatistics.decompressionInvocations}
            |       Bytes In: ${this.compressionStatistics.decompressionInputBytes.Bytes.toHumanReadableString()}
            |      Bytes Out: ${this.compressionStatistics.decompressionOutputBytes.Bytes.toHumanReadableString()}
        """.trimMargin()
    }


    class CursorStatistics(
        /** The name of this group of cursors. */
        val groupName: String,
        /** How many cursors have been opened so far? */
        val cursors: Long,
        /** How many times has "cursor.seekExactlyOrNext()" been called on a cursor? */
        val cursorExactlyOrNextSeeks: Long,
        /** How many times has "cursor.seekExactlyOrPrevious" been called on a cursor? */
        val cursorExactlyOrPreviousSeeks: Long,
        /** How many times has "cursor.first()" been called on a cursor? */
        val cursorFirstSeeks: Long,
        /** How many times has "cursor.last()" been called on a cursor? */
        val cursorLastSeeks: Long,
        /** How many times has "cursor.next()" been called on a cursor? */
        val cursorNextSeeks: Long,
        /** How many times has "cursor.previous()" been called on a cursor? */
        val cursorPreviousSeeks: Long,
    ) {

        val totalSeeks: Long
            get() = this.cursorExactlyOrNextSeeks +
                this.cursorExactlyOrPreviousSeeks +
                this.cursorFirstSeeks +
                this.cursorLastSeeks +
                this.cursorNextSeeks +
                this.cursorPreviousSeeks

        val totalMoves: Long
            get() = this.cursorNextSeeks +
                this.cursorPreviousSeeks

        val totalJumps: Long
            get() = this.cursorFirstSeeks +
                this.cursorLastSeeks

        val totalRandomSeek: Long
            get() = this.cursorExactlyOrNextSeeks +
                this.cursorExactlyOrPreviousSeeks

        operator fun plus(other: CursorStatistics): CursorStatistics {
            return CursorStatistics(
                groupName = this.groupName + " + " + other.groupName,
                cursors = this.cursors + other.cursors,
                cursorExactlyOrNextSeeks = this.cursorExactlyOrNextSeeks + other.cursorExactlyOrNextSeeks,
                cursorExactlyOrPreviousSeeks = this.cursorExactlyOrPreviousSeeks + other.cursorExactlyOrPreviousSeeks,
                cursorFirstSeeks = this.cursorFirstSeeks + other.cursorFirstSeeks,
                cursorLastSeeks = this.cursorLastSeeks + other.cursorLastSeeks,
                cursorNextSeeks = this.cursorNextSeeks + other.cursorNextSeeks,
                cursorPreviousSeeks = this.cursorPreviousSeeks + other.cursorPreviousSeeks,
            )
        }

        fun withName(newName: String): CursorStatistics {
            return CursorStatistics(
                groupName = newName,
                cursors = this.cursors,
                cursorExactlyOrNextSeeks = this.cursorExactlyOrNextSeeks,
                cursorExactlyOrPreviousSeeks = this.cursorExactlyOrPreviousSeeks,
                cursorFirstSeeks = this.cursorFirstSeeks,
                cursorLastSeeks = this.cursorLastSeeks,
                cursorNextSeeks = this.cursorNextSeeks,
                cursorPreviousSeeks = this.cursorPreviousSeeks,
            )
        }

    }

    class CacheStatistics(
        /** The name of the cache. */
        val cacheName: String,
        /** The number of times an entry has been evicted (invalidated) from the cache. */
        val evictions: Long,
        /** The number of cache misses that have occurred. */
        val misses: Long,
        /** The total number of requests recorded for this cache. */
        val requests: Long,
    ) {

        /** The number of cache hits that have occurred. */
        val hits: Long
            get() = requests - misses

        /**
         * The hit rate of the cache between 0% (0.0) and 100% (1.0).
         *
         * Returns 0.0 if no requests have occurred.
         */
        val hitRate: Double
            get() {
                val requests = requests
                return if (requests > 0) {
                    hits.toDouble() / requests
                } else {
                    0.0
                }
            }

    }

    class CompressionStatistics(
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
    )

}