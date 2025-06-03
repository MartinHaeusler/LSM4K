package org.chronos.chronostore.util.statistics

import org.chronos.chronostore.api.statistics.CursorStatisticsReport
import org.chronos.chronostore.util.cursor.Cursor
import java.util.concurrent.atomic.AtomicLong

/**
 * Collects statistics about cursors of a given [cursorType].
 */
class CursorStatisticsCollector(
    /** The type of cursor for which this collector gathers statistics. */
    val cursorType: String,
) {

    /** Indicates how many cursors of this [cursorType] have been opened. */
    val cursorsOpened = AtomicLong(0)

    /** Indicates how many cursors of this [cursorType] have been closed. */
    val cursorsClosed = AtomicLong(0)

    /** Indicates how many [Cursor.first] operations on cursors of this [cursorType] have been executed. */
    val cursorOperationsFirst = AtomicLong(0)

    /** Indicates how many [Cursor.last] operations on cursors of this [cursorType] have been executed. */
    val cursorOperationsLast = AtomicLong(0)

    /** Indicates how many [Cursor.next] operations on cursors of this [cursorType] have been executed. */
    val cursorOperationsNext = AtomicLong(0)

    /** Indicates how many [Cursor.previous] operations on cursors of this [cursorType] have been executed. */
    val cursorOperationsPrevious = AtomicLong(0)

    /** Indicates how many [Cursor.seekExactlyOrNext] operations on cursors of this [cursorType] have been executed. */
    val cursorOperationsSeekExactlyOrNext = AtomicLong(0)

    /** Indicates how many [Cursor.seekExactlyOrPrevious] operations on cursors of this [cursorType] have been executed. */
    val cursorOperationsSeekExactlyOrPrevious = AtomicLong(0)

    fun reportCursorOpened() {
        this.cursorsOpened.incrementAndGet()
    }

    fun reportCursorClosed() {
        this.cursorsClosed.incrementAndGet()
    }

    fun reportCursorOperationFirst() {
        this.cursorOperationsFirst.incrementAndGet()
    }

    fun reportCursorOperationLast() {
        this.cursorOperationsLast.incrementAndGet()
    }

    fun reportCursorOperationNext() {
        this.cursorOperationsNext.incrementAndGet()
    }

    fun reportCursorOperationPrevious() {
        this.cursorOperationsPrevious.incrementAndGet()
    }

    fun reportCursorOperationSeekExactlyOrNext() {
        this.cursorOperationsSeekExactlyOrNext.incrementAndGet()
    }

    fun reportCursorOperationSeekExactlyOrPrevious() {
        this.cursorOperationsSeekExactlyOrPrevious.incrementAndGet()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CursorStatisticsCollector

        return cursorType == other.cursorType
    }

    override fun hashCode(): Int {
        return cursorType.hashCode()
    }

    override fun toString(): String {
        return "CursorStatistics[${this.cursorType}]"
    }

    fun report(): CursorStatisticsReport {
        return CursorStatisticsReport(
            cursorType = this.cursorType,
            cursorsOpened = this.cursorsOpened.get(),
            cursorsClosed = this.cursorsClosed.get(),
            cursorOperationsFirst = this.cursorOperationsFirst.get(),
            cursorOperationsLast = this.cursorOperationsLast.get(),
            cursorOperationsNext = this.cursorOperationsNext.get(),
            cursorOperationsPrevious = this.cursorOperationsPrevious.get(),
            cursorOperationsSeekExactlyOrNext = this.cursorOperationsSeekExactlyOrNext.get(),
            cursorOperationsSeekExactlyOrPrevious = this.cursorOperationsSeekExactlyOrPrevious.get(),
        )
    }

}