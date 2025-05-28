package org.chronos.chronostore.util.statistics.report

import org.chronos.chronostore.util.cursor.Cursor

/**
 * An immutable snapshot report about cursors of a given [cursorType].
 */
class CursorStatisticsReport(
    /** The type of cursor for which this collector gathers statistics. */
    val cursorType: Class<out Cursor<*, *>>,

    /** Indicates how many cursors of this [cursorType] have been opened. */
    val cursorsOpened: Long,

    /** Indicates how many cursors of this [cursorType] have been closed. */
    val cursorsClosed: Long,

    /** Indicates how many [Cursor.first] operations on cursors of this [cursorType] have been executed. */
    val cursorOperationsFirst: Long,

    /** Indicates how many [Cursor.last] operations on cursors of this [cursorType] have been executed. */
    val cursorOperationsLast: Long,

    /** Indicates how many [Cursor.next] operations on cursors of this [cursorType] have been executed. */
    val cursorOperationsNext: Long,

    /** Indicates how many [Cursor.previous] operations on cursors of this [cursorType] have been executed. */
    val cursorOperationsPrevious: Long,

    /** Indicates how many [Cursor.seekExactlyOrNext] operations on cursors of this [cursorType] have been executed. */
    val cursorOperationsSeekExactlyOrNext: Long,

    /** Indicates how many [Cursor.seekExactlyOrPrevious] operations on cursors of this [cursorType] have been executed. */
    val cursorOperationsSeekExactlyOrPrevious: Long,
) {

    companion object {

        val EMPTY = empty(Cursor::class.java)

        fun empty(cursorType: Class<out Cursor<*, *>>): CursorStatisticsReport {
            return CursorStatisticsReport(
                cursorType = cursorType,
                cursorsOpened = 0L,
                cursorsClosed = 0L,
                cursorOperationsFirst = 0L,
                cursorOperationsLast = 0L,
                cursorOperationsNext = 0L,
                cursorOperationsPrevious = 0L,
                cursorOperationsSeekExactlyOrNext = 0L,
                cursorOperationsSeekExactlyOrPrevious = 0L,
            )
        }

        fun Collection<CursorStatisticsReport>.sum(): CursorStatisticsReport {
            return this.fold(EMPTY, CursorStatisticsReport::plus)
        }

    }

    val operations: Long
        get() {
            return this.cursorOperationsFirst +
                this.cursorOperationsLast +
                this.cursorOperationsNext +
                this.cursorOperationsPrevious +
                this.cursorOperationsSeekExactlyOrNext +
                this.cursorOperationsSeekExactlyOrPrevious
        }

    val seeks: Long
        get() {
            return this.cursorOperationsSeekExactlyOrNext +
                this.cursorOperationsSeekExactlyOrPrevious
        }

    val moves: Long
        get() {
            return this.cursorOperationsNext +
                this.cursorOperationsPrevious
        }

    val jumps: Long
        get() {
            return this.cursorOperationsFirst +
                this.cursorOperationsLast
        }

    operator fun plus(other: CursorStatisticsReport): CursorStatisticsReport {
        return CursorStatisticsReport(
            cursorType = Cursor::class.java,
            cursorsOpened = this.cursorsOpened + other.cursorsOpened,
            cursorsClosed = this.cursorsClosed + other.cursorsClosed,
            cursorOperationsFirst = this.cursorOperationsFirst + other.cursorOperationsFirst,
            cursorOperationsLast = this.cursorOperationsLast + other.cursorOperationsLast,
            cursorOperationsNext = this.cursorOperationsNext + other.cursorOperationsNext,
            cursorOperationsPrevious = this.cursorOperationsPrevious + other.cursorOperationsPrevious,
            cursorOperationsSeekExactlyOrNext = this.cursorOperationsSeekExactlyOrNext + other.cursorOperationsSeekExactlyOrNext,
            cursorOperationsSeekExactlyOrPrevious = this.cursorOperationsSeekExactlyOrPrevious + other.cursorOperationsSeekExactlyOrPrevious,
        )
    }
}