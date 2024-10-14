package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.util.Order
import org.chronos.chronostore.util.Order.ASCENDING
import org.chronos.chronostore.util.Order.DESCENDING
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics

open class OverlayCursor<C1 : Cursor<K, V>, C2 : Cursor<K, V>, K : Comparable<*>, V>(
    base: C1,
    overlay: C2,
) : CombiningCursor<C1, C2, K, V>(base, overlay) {

    override var keyOrNullInternal: K? = null
    override var valueOrNullInternal: V? = null

    init {
        ChronoStoreStatistics.OVERLAY_CURSORS.incrementAndGet()
    }

    override fun doFirst(): Boolean {
        ChronoStoreStatistics.OVERLAY_CURSOR_FIRST_SEEKS.incrementAndGet()
        // rewind the cursors to their first position. Both resulting keys are nullable,
        // because both collections may be empty, causing "first()" to fail.
        val baseKey = this.cursorA.firstAndReturnKey()
        this.updateLastModifiedCursorA()
        val overlayKey = this.cursorB.firstAndReturnKey()
        this.updateLastModifiedCursorB()

        return this.evaluatePosition(baseKey, overlayKey, ASCENDING, invalidateIfBothKeysAreNull = true)
    }

    override fun doLast(): Boolean {
        ChronoStoreStatistics.OVERLAY_CURSOR_LAST_SEEKS.incrementAndGet()
        // rewind the cursors to their first position. Both resulting keys are nullable, because
        // both collections may be empty, causing "last()" to fail.
        val baseKey = this.cursorA.lastAndReturnKey()
        this.updateLastModifiedCursorA()
        val overlayKey = this.cursorB.lastAndReturnKey()
        this.updateLastModifiedCursorB()

        return evaluatePosition(baseKey, overlayKey, DESCENDING, invalidateIfBothKeysAreNull = true)
    }

    override fun doMove(direction: Order): Boolean {
        when (direction) {
            ASCENDING -> ChronoStoreStatistics.OVERLAY_CURSOR_NEXT_SEEKS.incrementAndGet()
            DESCENDING -> ChronoStoreStatistics.OVERLAY_CURSOR_PREVIOUS_SEEKS.incrementAndGet()
        }

        val baseCursorKey = this.cursorA.moveUntilAfterKeyAndReturnKey(this.keyOrNull!!, direction)
        this.updateLastModifiedCursorA()
        val overlayCursorKey = this.cursorB.moveUntilAfterKeyAndReturnKey(this.keyOrNull!!, direction)
        this.updateLastModifiedCursorB()

        return this.evaluatePosition(baseCursorKey, overlayCursorKey, direction, invalidateIfBothKeysAreNull = false)
    }


    override fun doSeekExactlyOrPrevious(key: K): Boolean {
        ChronoStoreStatistics.OVERLAY_CURSOR_EXACTLY_OR_PREVIOUS_SEEKS.incrementAndGet()
        val baseCursorKey = if (this.cursorA.seekExactlyOrPrevious(key)) {
            this.cursorA.keyOrNull
        } else {
            null
        }
        this.updateLastModifiedCursorA()
        val overlayCursorKey = if (this.cursorB.seekExactlyOrPrevious(key)) {
            this.cursorB.keyOrNull
        } else {
            null
        }
        this.updateLastModifiedCursorB()
        // if only one of the two cursors has found nothing, we need to take an additional action in
        // order to enable next()/previous() in further steps: We have to rewind the failed cursor to the beginning.
        if (baseCursorKey == null && overlayCursorKey != null) {
            this.cursorA.first()
            this.updateLastModifiedCursorA()
        } else if (baseCursorKey != null && overlayCursorKey == null) {
            this.cursorB.first()
            this.updateLastModifiedCursorB()
        }

        return this.evaluatePosition(baseCursorKey, overlayCursorKey, DESCENDING, invalidateIfBothKeysAreNull = true)
    }


    override fun doSeekExactlyOrNext(key: K): Boolean {
        ChronoStoreStatistics.OVERLAY_CURSOR_EXACTLY_OR_NEXT_SEEKS.incrementAndGet()
        val baseCursorKey = if (this.cursorA.seekExactlyOrNext(key)) {
            this.cursorA.keyOrNull
        } else {
            null
        }
        this.updateLastModifiedCursorA()
        val overlayCursorKey = if (this.cursorB.seekExactlyOrNext(key)) {
            this.cursorB.keyOrNull
        } else {
            null
        }
        this.updateLastModifiedCursorB()
        // if only one of the two cursors has found nothing, we need to take an additional action in
        // order to enable next()/previous() in further steps: We have to rewind the failed cursor to the end.
        if (baseCursorKey == null && overlayCursorKey != null) {
            this.cursorA.last()
            this.updateLastModifiedCursorA()
        } else if (baseCursorKey != null && overlayCursorKey == null) {
            this.cursorB.last()
            this.updateLastModifiedCursorB()
        }

        return this.evaluatePosition(baseCursorKey, overlayCursorKey, ASCENDING, invalidateIfBothKeysAreNull = true)
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private fun evaluatePosition(baseKey: K?, overlayKey: K?, order: Order, invalidateIfBothKeysAreNull: Boolean): Boolean {
        if (baseKey == null && overlayKey == null) {
            return if (invalidateIfBothKeysAreNull) {
                // both cursors are empty -> this combined cursor has no keys to iterate over.
                this.markAsInvalidPosition()
            } else {
                // we remain in a valid position (i.e. key, value, and isValidPosition remain unchanged), we just can't go further.
                // This is the case when we hit the "edge" of the keyspace and next()/previous() is called in the direction for which
                // we have no more keys.
                return false
            }
        }

        val comparison = this.compareKeysNullsLast(baseKey, overlayKey, order)
        return if (comparison < 0) {
            // the base cursor has a predecessor key that doesn't appear in the overlay, use that.
            this.markAsValidPosition(baseKey!!, this.cursorA.valueOrNull)
        } else {
            // the base cursor either has the same key as the overlay, or the overlay has a key
            // that doesn't appear in the base cursor. Either way, we have to use the overlay cursor.
            this.markAsValidPosition(overlayKey!!, this.cursorB.valueOrNull)
        }
    }

    private fun markAsInvalidPosition(): Boolean {
        this.isValidPosition = false
        this.keyOrNullInternal = null
        this.valueOrNullInternal = null
        return false
    }

    private fun markAsValidPosition(key: K, value: V?): Boolean {
        this.isValidPosition = true
        this.keyOrNullInternal = key
        this.valueOrNullInternal = value
        return true
    }

    @Suppress("UNCHECKED_CAST")
    private fun compareKeysNullsLast(key1: K?, key2: K?, order: Order): Int {
        return when {
            key1 == null && key2 == null -> 0
            key1 != null && key2 == null -> -1
            key1 == null && key2 != null -> +1
            else -> when (order) {
                ASCENDING -> (key1 as Comparable<Any>).compareTo(key2!!)
                DESCENDING -> (key2 as Comparable<Any>).compareTo(key1!!)
            }
        }
    }

    private fun Cursor<K, V>.firstAndReturnKey(): K? {
        return if (this.first()) {
            this.keyOrNull
        } else {
            null
        }
    }

    private fun Cursor<K, V>.lastAndReturnKey(): K? {
        return if (this.last()) {
            this.keyOrNull
        } else {
            null
        }
    }

    private fun Cursor<K, V>.moveUntilAfterKeyAndReturnKey(key: K, order: Order): K? {
        if (!this.isValidPosition) {
            return null
        }
        while (compareKeysNullsLast(this.keyOrNull, key, order) <= 0) {
            if (!this.move(order)) {
                return null
            }
        }
        return this.keyOrNull
    }

    override fun toString(): String {
        return "OverlayCursor[Base: ${this.cursorA}, Overlay: ${this.cursorB}]"
    }

}