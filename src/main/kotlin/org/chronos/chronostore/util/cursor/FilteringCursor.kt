package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.util.Order
import org.chronos.chronostore.util.Order.ASCENDING
import org.chronos.chronostore.util.Order.DESCENDING

abstract class FilteringCursor<C : Cursor<K, V>, K, V>(
    innerCursor: C,
) : WrappingCursor<C, K, V>(innerCursor) {

    override val keyOrNullInternal: K?
        // we only ever stop the inner cursor at locations which
        // match the filter. So we can safely pass the inner key directly.
        get() = this.innerCursor.keyOrNull

    override val valueOrNullInternal: V?
        // we only ever stop the inner cursor at locations which
        // match the filter. So we can safely pass the inner value directly.
        get() = this.innerCursor.valueOrNull

    override fun doFirst(): Boolean {
        return if (this.innerCursor.first()) {
            if (this.isInnerPositionMatchingTheFilter) {
                true
            } else {
                this.moveUntilFilterIsMatched(ASCENDING, returnToInitialPositionIfNotFound = false)
            }
        } else {
            false
        }
    }

    override fun doLast(): Boolean {
        return if (this.innerCursor.last()) {
            if (this.isInnerPositionMatchingTheFilter) {
                true
            } else {
                this.moveUntilFilterIsMatched(DESCENDING, returnToInitialPositionIfNotFound = false)
            }
        } else {
            false
        }
    }

    override fun doMove(direction: Order): Boolean {
        return this.moveUntilFilterIsMatched(direction, returnToInitialPositionIfNotFound = true)
    }

    override fun doSeekExactlyOrPrevious(key: K): Boolean {
        return if (!this.innerCursor.seekExactlyOrPrevious(key)) {
            // inner cursor found nothing
            false
        } else if (this.isInnerPositionMatchingTheFilter) {
            // inner cursor found something that matches our filter -> ok
            true
        } else {
            // inner cursor found something, but it didn't match our filter,
            // move to the previous element until we either find a key which
            // matches the filter, or run out of keys
            this.moveUntilFilterIsMatched(DESCENDING, returnToInitialPositionIfNotFound = false)
        }
    }

    override fun doSeekExactlyOrNext(key: K): Boolean {
        return if (!this.innerCursor.seekExactlyOrNext(key)) {
            // inner cursor found nothing
            false
        } else if (this.isInnerPositionMatchingTheFilter) {
            // inner cursor found something that matches our filter -> ok
            true
        } else {
            // inner cursor found something, but it didn't match our filter,
            // move to the next element until we either find a key which
            // matches the filter, or run out of keys
            this.moveUntilFilterIsMatched(ASCENDING, returnToInitialPositionIfNotFound = false)
        }
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private fun moveUntilFilterIsMatched(direction: Order, returnToInitialPositionIfNotFound: Boolean): Boolean {
        // we might need to return to our current position, so keep track of how many steps we took.
        var moves = 0
        do {
            if (!this.innerCursor.move(direction)) {
                // inner cursor is exhausted. Do we need to return to the initial position?
                if (returnToInitialPositionIfNotFound) {
                    repeat(moves) {
                        this.innerCursor.move(direction.inverse)
                    }
                }
                return false
            } else {
                moves++
            }
        } while (!this.isInnerPositionMatchingTheFilter)
        return true
    }

    // =================================================================================================================
    // ABSTRACT METHOD DECLARATIONS
    // =================================================================================================================

    protected abstract val isInnerPositionMatchingTheFilter: Boolean

}