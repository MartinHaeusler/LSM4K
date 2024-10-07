package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.util.Order

abstract class CombiningCursor<C1 : Cursor<*, *>, C2 : Cursor<*, *>, K, V>(
    protected val cursorA: C1,
    protected val cursorB: C2,
) : AbstractCursor<K, V>() {

    private var lastSeenModCountA = this.cursorA.modCount
    private var lastSeenModCountB = this.cursorB.modCount

    override fun invalidatePosition() {
        super.invalidatePosition()
        this.cursorA.invalidatePosition()
        this.cursorB.invalidatePosition()
        this.lastSeenModCountA = this.cursorA.modCount
        this.lastSeenModCountB = this.cursorB.modCount
    }

    final override fun firstInternal(): Boolean {
        val result = this.doFirst()
        this.lastSeenModCountA = this.cursorA.modCount
        this.lastSeenModCountB = this.cursorB.modCount
        return result
    }

    final override fun lastInternal(): Boolean {
        val result = this.doLast()
        this.lastSeenModCountA = this.cursorA.modCount
        this.lastSeenModCountB = this.cursorB.modCount
        return result
    }

    final override fun moveInternal(direction: Order): Boolean {
        val result = this.doMove(direction)
        this.lastSeenModCountA = this.cursorA.modCount
        this.lastSeenModCountB = this.cursorB.modCount
        return result
    }

    final override fun seekExactlyOrPreviousInternal(key: K): Boolean {
        val result = this.doSeekExactlyOrPrevious(key)
        this.lastSeenModCountA = this.cursorA.modCount
        this.lastSeenModCountB = this.cursorB.modCount
        return result
    }

    override fun seekExactlyOrNextInternal(key: K): Boolean {
        val result = this.doSeekExactlyOrNext(key)
        this.lastSeenModCountA = this.cursorA.modCount
        this.lastSeenModCountB = this.cursorB.modCount
        return result
    }

    override fun closeInternal() {
        CursorUtils.executeCloseHandlers(this.cursorA::close, this.cursorB::close)
    }

    override fun checkInvariants() {
        super.checkInvariants()
        if (this.lastSeenModCountA != this.cursorA.modCount) {
            throw IllegalStateException(
                "The inner cursor has been moved independently of the wrapper! Please do not move" +
                    " the inner cursor directly after wrapping it into another cursor!" +
                    " The wrapper is: '${this}' (expected mod count: ${this.lastSeenModCountA}), the inner cursor is: '${this.cursorA}' (actual mod count: ${this.cursorA.modCount})"
            )
        }
        if (this.lastSeenModCountB != this.cursorB.modCount) {
            throw IllegalStateException(
                "The inner cursor has been moved independently of the wrapper! Please do not move" +
                    " the inner cursor directly after wrapping it into another cursor!" +
                    " The wrapper is: '${this}' (expected mod count: ${this.lastSeenModCountB}), the inner cursor is: '${this.cursorB}'  (actual mod count: ${this.cursorB.modCount})"
            )
        }
    }

    override fun toString(): String {
        return "CombiningCursor[A: '${this.cursorA}', B: '${this.cursorB}']"
    }

    protected fun updateLastModifiedCursorA() {
        this.lastSeenModCountA = this.cursorA.modCount
    }

    protected fun updateLastModifiedCursorB() {
        this.lastSeenModCountB = this.cursorB.modCount
    }

    // =================================================================================================================
    // ABSTRACT METHOD DECLARATIONS
    // =================================================================================================================

    protected abstract fun doFirst(): Boolean

    protected abstract fun doLast(): Boolean

    protected abstract fun doMove(direction: Order): Boolean

    protected abstract fun doSeekExactlyOrPrevious(key: K): Boolean

    protected abstract fun doSeekExactlyOrNext(key: K): Boolean

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

}