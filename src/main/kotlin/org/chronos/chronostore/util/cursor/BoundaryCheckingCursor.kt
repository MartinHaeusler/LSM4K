package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.util.Order

class BoundaryCheckingCursor<K : Comparable<*>, V>(
    innerCursor: Cursor<K, V>,
) : WrappingCursor<Cursor<K, V>, K, V>(
    innerCursor
) {

    private var mayHaveNext: Boolean = true
    private var mayHavePrevious: Boolean = true

    override fun doFirst(): Boolean {
        val result = this.innerCursor.first()
        // if the move succeeded, there may be a next element
        // if the move failed, the position is invalid anyway
        this.mayHaveNext = result
        // at first position, there's NEVER a previous.
        this.mayHavePrevious = false
        return result
    }

    override fun doLast(): Boolean {
        val result = this.innerCursor.last()
        // if the move succeeded, there may be a next element
        // if the move failed, the position is invalid anyway
        this.mayHavePrevious = result
        // at first position, there's NEVER a previous.
        this.mayHaveNext = false
        return result
    }

    override fun doMove(direction: Order): Boolean {
        return when (direction) {
            Order.ASCENDING -> {
                if (!this.mayHaveNext) {
                    // don't bother the cursor
                    false
                } else {
                    val result = this.innerCursor.next()
                    // if the move to next failed, there is no next element
                    // if the move to next succeeded, there may be another next element.
                    this.mayHaveNext = result
                    result
                }
            }

            Order.DESCENDING -> {
                if (!this.mayHavePrevious) {
                    // don't bother the cursor
                    false
                } else {
                    val result = this.innerCursor.previous()
                    // if the move to previous failed, there is no previous element
                    // if the move to previous succeeded, there may be another previous element.
                    this.mayHavePrevious = result
                    result
                }
            }
        }
    }

    override fun doSeekExactlyOrNext(key: K): Boolean {
        val result = this.innerCursor.seekExactlyOrNext(key)
        this.mayHaveNext = result
        this.mayHavePrevious = result
        return result
    }

    override fun doSeekExactlyOrPrevious(key: K): Boolean {
        val result = this.innerCursor.seekExactlyOrPrevious(key)
        this.mayHaveNext = result
        this.mayHavePrevious = result
        return result
    }

    override val keyOrNullInternal: K?
        get() = this.innerCursor.keyOrNull

    override val valueOrNullInternal: V?
        get() = this.innerCursor.valueOrNull

}