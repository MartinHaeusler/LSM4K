package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.util.Order

abstract class WrappingCursor<C : Cursor<*, *>, K, V>(
    protected val innerCursor: C
) : AbstractCursor<K, V>() {

    private var lastSeenModCount = this.innerCursor.modCount

    init {
        if (!this.innerCursor.isOpen) {
            throw IllegalStateException("Cannot create Key-Filtered Cursor - the given cursor is already closed: ${this.innerCursor}")
        }
    }

    override fun invalidatePosition() {
        super.invalidatePosition()
        this.innerCursor.invalidatePosition()
        this.lastSeenModCount = this.innerCursor.modCount
    }

    final override fun firstInternal(): Boolean {
        val result = this.doFirst()
        this.lastSeenModCount = this.innerCursor.modCount
        return result
    }

    final override fun lastInternal(): Boolean {
        val result = this.doLast()
        this.lastSeenModCount = this.innerCursor.modCount
        return result
    }

    final override fun moveInternal(direction: Order): Boolean {
        val result = this.doMove(direction)
        this.lastSeenModCount = this.innerCursor.modCount
        return result
    }

    final override fun seekExactlyOrPreviousInternal(key: K): Boolean {
        val result = this.doSeekExactlyOrPrevious(key)
        this.lastSeenModCount = this.innerCursor.modCount
        return result
    }

    override fun seekExactlyOrNextInternal(key: K): Boolean {
        val result = this.doSeekExactlyOrNext(key)
        this.lastSeenModCount = this.innerCursor.modCount
        return result
    }

    override fun closeInternal() {
        this.innerCursor.close()
    }

    override fun toString(): String {
        return "WrappingCursor['${this.innerCursor}']"
    }

    // =================================================================================================================
    // ABSTRACT METHOD DECLARATIONS
    // =================================================================================================================

    protected abstract fun doFirst(): Boolean

    protected abstract fun doLast(): Boolean

    protected abstract fun doMove(direction: Order): Boolean

    protected abstract fun doSeekExactlyOrPrevious(key: K): Boolean

    protected abstract fun doSeekExactlyOrNext(key: K): Boolean

}