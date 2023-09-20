package org.chronos.chronostore.util.statistics

import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.cursor.OverlayCursor
import org.chronos.chronostore.util.cursor.VersioningCursor
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
    /** How many pages needed to be loaded from disk? */
    val pageLoadsFromDisk: Long,
    /** How many times did we have to load a file header from disk?  */
    val fileHeaderLoadsFromDisk: Long,
    /** How long have writer threads been stalled because of full in-memory trees? */
    val totalWriteStallTimeMillis: Long,
    /** How many times have writer threads been stalled because of full in-memory trees? */
    val writeStallEvents: Long,
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

        /** How many pages needed to be loaded from disk? */
        val PAGE_LOADS_FROM_DISK = AtomicLong(0L)

        /** How many times did we have to load a file header from disk?  */
        val FILE_HEADER_LOADS_FROM_DISK = AtomicLong(0L)

        /** How long have writer threads been stalled because of full in-memory trees? */
        val TOTAL_WRITE_STALL_TIME_MILLIS = AtomicLong(0L)

        /** How many times have writer threads been stalled because of full in-memory trees? */
        val WRITE_STALL_EVENTS = AtomicLong(0L)

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
                pageLoadsFromDisk = PAGE_LOADS_FROM_DISK.get(),
                fileHeaderLoadsFromDisk = FILE_HEADER_LOADS_FROM_DISK.get(),
                totalWriteStallTimeMillis = TOTAL_WRITE_STALL_TIME_MILLIS.get(),
                writeStallEvents = WRITE_STALL_EVENTS.get(),
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
            PAGE_LOADS_FROM_DISK.set(0L)
            FILE_HEADER_LOADS_FROM_DISK.set(0L)
            TOTAL_WRITE_STALL_TIME_MILLIS.set(0L)
            WRITE_STALL_EVENTS.set(0L)
        }
    }

    val cursorGroups = listOf(this.fileCursorStatistics, this.overlayCursorStatistics, this.versioningCursorStatistics)

    val cursorGroupNameToStatistics = cursorGroups.associateBy { it.groupName }

    val allCursorStatistics: CursorStatistics = cursorGroups.reduce(CursorStatistics::plus).withName("All Cursors")


    fun prettyPrint(): String {
        return """ChronoStore Statistics
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
            | 
            | Header Loads: ${this.fileHeaderLoadsFromDisk}
            | Page Loads: ${this.pageLoadsFromDisk}
            | Write Stall Time: ${this.totalWriteStallTimeMillis}ms
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
}