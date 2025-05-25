package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.util.Order
import org.chronos.chronostore.util.Order.ASCENDING
import org.chronos.chronostore.util.Order.DESCENDING
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics

open class OverlayCursor<K : Comparable<*>, V>(
    base: Cursor<K, V>,
    overlay: Cursor<K, V?>,
) : CombiningCursor<Cursor<K, V>, Cursor<K, V?>, K, V>(
    // apply "boundary checking" to the sub-cursors to avoid calling their "next()" and "previous()"
    // methods too often. This results in efficiency gains if one of the cursors contains much more
    // entries than the other, causing the smaller one to call "next()" / "previous()" constantly while
    // returning false all the time.
    cursorA = BoundaryCheckingCursor(base),
    cursorB = BoundaryCheckingCursor(overlay)
) {

    companion object {

        @Suppress("UNCHECKED_CAST")
        @JvmName("overlayOntoNonNullable")
        fun <K : Comparable<*>, V> Cursor<K, V>.overlayOnto(base: Cursor<K, V>): Cursor<K, V> {
            return OverlayCursor(base = base, overlay = this as Cursor<K, V?>)
        }

        @JvmName("overlayOntoNullable")
        fun <K : Comparable<*>, V> Cursor<K, V?>.overlayOnto(base: Cursor<K, V>): Cursor<K, V> {
            return OverlayCursor(base = base, overlay = this)
        }

        fun <K : Comparable<*>, V> Cursor<K, V>.overlayUnder(overlay: Cursor<K, V?>): Cursor<K, V> {
            return OverlayCursor(base = this, overlay = overlay)
        }

    }

    override var keyOrNullInternal: K? = null
    override var valueOrNullInternal: V? = null

    init {
        ChronoStoreStatistics.OVERLAY_CURSORS.incrementAndGet()
    }

    override fun doFirst(): Boolean {
        ChronoStoreStatistics.OVERLAY_CURSOR_FIRST_SEEKS.incrementAndGet()
        // rewind the cursors to their first position. Both resulting keys are nullable,
        // because both collections may be empty, causing "first()" to fail.
        this.cursorA.firstAndReturnKey()
        this.updateLastModifiedCursorA()

        this.cursorB.firstAndReturnKey()
        this.updateLastModifiedCursorB()

        // did the overlay cursor remove our first key?
        val found = skipAcrossDeletionsInOverlay(ASCENDING)
        if (!found) {
            this.invalidatePosition()
            return false
        }

        return this.evaluatePosition(
            baseKey = this.cursorA.keyOrNull,
            overlayKey = this.cursorB.keyOrNull,
            order = ASCENDING,
            invalidateIfBothKeysAreNull = true,
        )
    }

    override fun doLast(): Boolean {
        ChronoStoreStatistics.OVERLAY_CURSOR_LAST_SEEKS.incrementAndGet()
        // rewind the cursors to their first position. Both resulting keys are nullable, because
        // both collections may be empty, causing "last()" to fail.
        this.cursorA.lastAndReturnKey()
        this.updateLastModifiedCursorA()

        this.cursorB.lastAndReturnKey()
        this.updateLastModifiedCursorB()

        // did the overlay cursor remove our last key?
        val found = skipAcrossDeletionsInOverlay(DESCENDING)
        if (!found) {
            this.invalidatePosition()
            return false
        }

        return evaluatePosition(
            baseKey = this.cursorA.keyOrNull,
            overlayKey = this.cursorB.keyOrNull,
            order = DESCENDING,
            invalidateIfBothKeysAreNull = true,
        )
    }

    override fun doMove(direction: Order): Boolean {
        when (direction) {
            ASCENDING -> ChronoStoreStatistics.OVERLAY_CURSOR_NEXT_SEEKS.incrementAndGet()
            DESCENDING -> ChronoStoreStatistics.OVERLAY_CURSOR_PREVIOUS_SEEKS.incrementAndGet()
        }
        if (!this.isValidPosition) {
            // we're not in a valid position, therefore moving doesn't do
            // anything by definition.
            return false
        }

        val startKey = this.key

        val currentBaseKey = this.cursorA.keyOrNull
        val currentOverlayKey = this.cursorB.keyOrNull
        val keyCmp = this.compareKeysNullsLast(currentBaseKey, currentOverlayKey, direction)

        return when {
            keyCmp == 0 -> doMoveSameInitialKey(direction, currentBaseKey, currentOverlayKey)
            keyCmp < 0 -> doMoveBaseBehind(direction, startKey, currentBaseKey, currentOverlayKey)
            else -> doMoveBaseAhead(direction, startKey, currentBaseKey, currentOverlayKey)
        }
    }

    private fun doMoveSameInitialKey(direction: Order, currentBaseKey: K?, currentOverlayKey: K?): Boolean {
        // both cursors are at the same key. Move them both once
        val baseMoved = this.cursorA.move(direction)
        val overlayMoved = this.cursorB.move(direction)

        if (!baseMoved && !overlayMoved) {
            // if neither cursor moved, both are at their last entry.
            // -> we have nowhere to go.
            return false
        } else if (!baseMoved && overlayMoved) {
            // base is out of options, use whatever overlay says (but skip over nulls)
            while (this.cursorB.valueOrNull == null) {
                if (!this.cursorB.move(direction)) {
                    // we're out of data
                    if (currentBaseKey != null) {
                        this.cursorA.seekExactlyOrNext(currentBaseKey)
                        this.cursorB.seekExactlyOrNext(currentBaseKey)
                    }
                    this.updateLastModifiedCursorA()
                    this.updateLastModifiedCursorB()
                    return false
                }
            }

            this.markAsValidPosition(this.cursorB.key, this.cursorB.value)
            this.updateLastModifiedCursorA()
            this.updateLastModifiedCursorB()
            return true
        } else if (baseMoved && !overlayMoved) {
            // overlay is out of options, use whatever the base says
            this.markAsValidPosition(this.cursorA.key, this.cursorA.value)
            this.updateLastModifiedCursorA()
            this.updateLastModifiedCursorB()

            return true
        } else {
            if (!this.skipAcrossDeletionsInOverlay(direction)) {
                // due to deletions in the overlay, we don't have another valid
                // entry to go to. In order to honor the contract of "move(...)"
                // (which doesn't modify anything observably if it returns false),
                // we reset our internal cursors.
                if (currentBaseKey != null) {
                    this.cursorA.seekExactlyOrNext(currentBaseKey)
                }
                if (currentOverlayKey != null) {
                    this.cursorB.seekExactlyOrNext(currentOverlayKey)
                }

                // we can't go in this direction.
                return false
            }

            this.updateLastModifiedCursorA()
            this.updateLastModifiedCursorB()

            return this.evaluatePosition(
                baseKey = this.cursorA.keyOrNull,
                overlayKey = this.cursorB.keyOrNull,
                order = direction,
                invalidateIfBothKeysAreNull = false,
            )
        }
    }


    private fun doMoveBaseAhead(direction: Order, startKey: K, currentBaseKey: K?, currentOverlayKey: K?): Boolean {
        // the base cursor is "ahead".
        val overlayMoved = this.cursorB.move(direction)

        if (!overlayMoved) {
            // the overlay cursor is exhausted. This can happen if the overlay cursor
            // has more entries at the end of the range of the base cursor.
            val baseMoved = this.cursorA.move(direction)
            if (!baseMoved) {

                val baseKey = this.cursorA.keyOrNull
                if (this.compareKeysNullsLast(startKey, baseKey, direction) == 0) {
                    // if neither cursor moved, both are at their last entry.
                    // -> we have nowhere to go.
                    return false
                }

                // we have one last key to report, which is the key in base (which was ahead)
                if (baseKey != null) {
                    this.keyOrNullInternal = baseKey
                    this.valueOrNullInternal = this.cursorA.valueOrNull
                    return true
                }

                return false
            }
            if (!this.skipAcrossDeletionsInOverlay(direction)) {
                // due to deletions in the overlay, we don't have another valid
                // entry to go to. In order to honor the contract of "move(...)"
                // (which doesn't modify anything observably if it returns false),
                // we reset our internal cursors.
                if (currentBaseKey != null) {
                    this.cursorA.seekExactlyOrNext(currentBaseKey)
                }
                if (currentOverlayKey != null) {
                    this.cursorB.seekExactlyOrNext(currentOverlayKey)
                }
                this.updateLastModifiedCursorA()
                this.updateLastModifiedCursorB()

                // we can't go in this direction.
                return false
            }

            this.markAsValidPosition(this.cursorA.key, this.cursorA.value)
            this.updateLastModifiedCursorA()
            this.updateLastModifiedCursorB()

            return true
        } else {
            // we've managed to move the overlay cursor. It may now happen that the
            // base cursor key coincides with the overlay cursor key and the overlay
            // is a deletion. Move them if that's the case.
            if (!this.skipAcrossDeletionsInOverlay(direction)) {
                // due to deletions in the overlay, we don't have another valid
                // entry to go to. In order to honor the contract of "move(...)"
                // (which doesn't modify anything observably if it returns false),
                // we reset our internal cursors.
                if (currentBaseKey != null) {
                    this.cursorA.seekExactlyOrNext(currentBaseKey)
                }
                if (currentOverlayKey != null) {
                    this.cursorB.seekExactlyOrNext(currentOverlayKey)
                }
                this.updateLastModifiedCursorA()
                this.updateLastModifiedCursorB()

                // we can't go in this direction.
                return false
            }

            this.updateLastModifiedCursorA()
            this.updateLastModifiedCursorB()

            return this.evaluatePosition(
                baseKey = this.cursorA.keyOrNull,
                overlayKey = this.cursorB.keyOrNull,
                order = direction,
                invalidateIfBothKeysAreNull = false,
            )
        }

    }

    private fun doMoveBaseBehind(direction: Order, startKey: K, currentBaseKey: K?, currentOverlayKey: K?): Boolean {
        // the base cursor is "behind".
        val baseMoved = this.cursorA.move(direction)

        if (!baseMoved) {
            // the base cursor is exhausted. This can happen if the overlay cursor
            // has more entries at the end of the range of the base cursor.
            val overlayMoved = this.cursorB.move(direction)
            if (!overlayMoved) {
                val overlayKey = this.cursorB.keyOrNull

                // we have potentially one last key to report, which is the key in overlay (which was ahead)
                if (this.compareKeysNullsLast(startKey, overlayKey, direction) == 0) {
                    // if neither cursor moved, both are at their last entry.
                    // -> we have nowhere to go.
                    return false
                }

                val overlayValue = this.cursorB.valueOrNull
                if (overlayKey != null && overlayValue != null) {
                    this.keyOrNullInternal = overlayKey
                    this.valueOrNullInternal = overlayValue
                    return true
                } else {
                    return false
                }
            }

            if (!this.skipAcrossDeletionsInOverlay(direction)) {
                // due to deletions in the overlay, we don't have another valid
                // entry to go to. In order to honor the contract of "move(...)"
                // (which doesn't modify anything observably if it returns false),
                // we reset our internal cursors.
                if (currentBaseKey != null) {
                    this.cursorA.seekExactlyOrNext(currentBaseKey)
                }
                if (currentOverlayKey != null) {
                    this.cursorB.seekExactlyOrNext(currentOverlayKey)
                }
                this.updateLastModifiedCursorA()
                this.updateLastModifiedCursorB()

                // we can't go in this direction.
                return false
            }

            while (this.cursorB.valueOrNull == null) {
                if (!this.cursorB.move(direction)) {
                    if (currentBaseKey != null) {
                        this.cursorA.seekExactlyOrNext(currentBaseKey)
                        this.cursorB.seekExactlyOrNext(currentBaseKey)
                    }
                    this.updateLastModifiedCursorA()
                    this.updateLastModifiedCursorB()
                    return false
                }
            }

            this.markAsValidPosition(this.cursorB.key, this.cursorB.value)
            this.updateLastModifiedCursorA()
            this.updateLastModifiedCursorB()
            return true
        }

        // we've managed to move the base cursor. It may now happen that the
        // base cursor key coincides with the overlay cursor key and the overlay
        // is a deletion. Move them if that's the case.
        if (!this.skipAcrossDeletionsInOverlay(direction)) {
            // due to deletions in the overlay, we don't have another valid
            // entry to go to. In order to honor the contract of "move(...)"
            // (which doesn't modify anything observably if it returns false),
            // we reset our internal cursors.
            if (currentBaseKey != null) {
                this.cursorA.seekExactlyOrNext(currentBaseKey)
            }
            if (currentOverlayKey != null) {
                this.cursorB.seekExactlyOrNext(currentOverlayKey)
            }
            this.updateLastModifiedCursorA()
            this.updateLastModifiedCursorB()

            // we can't go in this direction.
            return false
        }

        this.updateLastModifiedCursorA()
        this.updateLastModifiedCursorB()

        return this.evaluatePosition(
            baseKey = this.cursorA.keyOrNull,
            overlayKey = this.cursorB.keyOrNull,
            order = direction,
            invalidateIfBothKeysAreNull = false,
        )
    }

    override fun doSeekExactlyOrPrevious(key: K): Boolean {
        ChronoStoreStatistics.OVERLAY_CURSOR_EXACTLY_OR_PREVIOUS_SEEKS.incrementAndGet()
        var baseCursorKey = if (this.cursorA.seekExactlyOrPrevious(key)) {
            this.cursorA.keyOrNull
        } else {
            null
        }
        this.updateLastModifiedCursorA()
        var overlayCursorKey = if (this.cursorB.seekExactlyOrPrevious(key)) {
            this.cursorB.keyOrNull
        } else {
            null
        }
        this.updateLastModifiedCursorB()

        if (!this.skipAcrossDeletionsInOverlay(DESCENDING)) {
            this.invalidatePosition()
            return false
        }

        baseCursorKey = this.cursorA.keyOrNull
        overlayCursorKey = this.cursorB.keyOrNull

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
        var baseCursorKey = if (this.cursorA.seekExactlyOrNext(key)) {
            this.cursorA.keyOrNull
        } else {
            null
        }
        this.updateLastModifiedCursorA()
        var overlayCursorKey = if (this.cursorB.seekExactlyOrNext(key)) {
            this.cursorB.keyOrNull
        } else {
            null
        }
        this.updateLastModifiedCursorB()

        if (!this.skipAcrossDeletionsInOverlay(ASCENDING)) {
            this.invalidatePosition()
            return false
        }

        baseCursorKey = this.cursorA.keyOrNull
        overlayCursorKey = this.cursorB.keyOrNull

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

    private fun skipAcrossDeletionsInOverlay(direction: Order): Boolean {
        while (true) {
            // first of all, skip deletions in the overlay cursor until the overlay cursor has the same key as base or is a head of base.
            var changed = this.skipDeletionsInOverlayUntilSameAsOrAheadOfBase(direction)

            // then, check if a deletion coincides with the base cursor. If so, skip ahead until the deletions no longer coincide
            // and the two cursors can either produce a key, or have nowhere left to go.
            while (this.cursorA.keyOrNull == this.cursorB.keyOrNull && this.cursorB.valueOrNull == null) {
                // the overlay cursor removed this key. Move both cursors forward
                val cursorAMoved = this.cursorA.move(direction)
                val cursorBMoved = this.cursorB.move(direction)

                changed = true

                if (!cursorAMoved && !cursorBMoved) {
                    // the overlay cursor eliminates all keys in the base cursor -> nothing left!
                    this.updateLastModifiedCursorA()
                    this.updateLastModifiedCursorB()
                    return false
                }
            }

            if (!changed) {
                // skipping ahead changed nothing, and the same-key-elimination didn't apply
                break
            }
        }
        // we've found a position that seems valid.
        this.updateLastModifiedCursorA()
        this.updateLastModifiedCursorB()
        return true
    }

    private fun skipDeletionsInOverlayUntilSameAsOrAheadOfBase(direction: Order): Boolean {
        var changed = false
        while (compareKeysNullsLast(this.cursorA.keyOrNull, this.cursorB.keyOrNull, direction) > 0 && this.cursorB.valueOrNull == null) {
            changed = true
            // the overlay cursor contains deletions for which there is no corresponding entry in the base,
            // skip them.
            if (!cursorB.move(direction)) {
                // overlay cursor ran out of entries
                cursorB.invalidatePosition()
                return true
            }
        }
        return changed
    }

    private fun evaluatePosition(baseKey: K?, overlayKey: K?, order: Order, invalidateIfBothKeysAreNull: Boolean): Boolean {
        if (baseKey == null && overlayKey == null) {
            return if (invalidateIfBothKeysAreNull) {
                // both cursors are empty -> this combined cursor has no keys to iterate over.
                this.markAsInvalidPosition()
            } else {
                // we remain in a valid position (i.e. key, value, and isValidPosition remain unchanged), we just can't go further.
                // This is the case when we hit the "edge" of the keyspace and next()/previous() is called in the direction for which
                // we have no more keys.
                false
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


    @JvmName("firstAndReturnKeyNonNullable")
    private fun Cursor<K, V>.firstAndReturnKey(): K? {
        return if (this.first()) {
            this.keyOrNull
        } else {
            null
        }
    }

    @JvmName("firstAndReturnKeyNullable")
    private fun Cursor<K, V?>.firstAndReturnKey(): K? {
        return if (this.first()) {
            this.keyOrNull
        } else {
            null
        }
    }

    @JvmName("lastAndReturnKeyNonNullable")
    private fun Cursor<K, V>.lastAndReturnKey(): K? {
        return if (this.last()) {
            this.keyOrNull
        } else {
            null
        }
    }

    @JvmName("lastAndReturnKeyNullable")
    private fun Cursor<K, V?>.lastAndReturnKey(): K? {
        return if (this.last()) {
            this.keyOrNull
        } else {
            null
        }
    }

    override fun toString(): String {
        return "OverlayCursor[Base: ${this.cursorA}, Overlay: ${this.cursorB}]"
    }

}