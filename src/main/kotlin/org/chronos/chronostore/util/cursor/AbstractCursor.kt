package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.util.Order
import org.chronos.chronostore.util.Order.ASCENDING
import org.chronos.chronostore.util.Order.DESCENDING

abstract class AbstractCursor<K, V> : Cursor<K, V> {

    override var isOpen: Boolean = true
    override var modCount: Long = 0
    override var isValidPosition: Boolean = false

    private val closeHandlers = mutableListOf<CloseHandler>()

    override fun invalidatePosition() {
        this.checkInvariants()
        this.isValidPosition = false
        this.modCount++
    }

    final override val keyOrNull: K?
        get() {
            this.checkInvariants()
            return if (!this.isValidPosition) {
                null
            } else {
                this.keyOrNullInternal
            }
        }

    final override val valueOrNull: V?
        get() {
            this.checkInvariants()
            return if (!this.isValidPosition) {
                null
            } else {
                this.valueOrNullInternal
            }
        }

    final override fun first(): Boolean {
        this.checkInvariants()
        this.isValidPosition = this.firstInternal()
        this.modCount++
        return this.isValidPosition
    }

    final override fun last(): Boolean {
        this.checkInvariants()
        this.isValidPosition = this.lastInternal()
        this.modCount++
        return this.isValidPosition
    }

    final override fun next(): Boolean {
        return this.move(ASCENDING)
    }

    final override fun previous(): Boolean {
        return this.move(DESCENDING)
    }

    final override fun seekExactlyOrNext(key: K): Boolean {
        this.checkInvariants()
        if (key == this.keyOrNull) {
            // we're already there
            return true
        }
        this.isValidPosition = this.seekExactlyOrNextInternal(key)
        this.modCount++
        return this.isValidPosition
    }

    final override fun seekExactlyOrPrevious(key: K): Boolean {
        this.checkInvariants()
        if (key == this.keyOrNull) {
            // we're already there
            return true
        }
        this.isValidPosition = this.seekExactlyOrPreviousInternal(key)
        this.modCount++
        return this.isValidPosition
    }

    final override fun move(direction: Order): Boolean {
        this.checkInvariants()
        if (!this.isValidPosition) {
            return false
        }
        // moving never changes the validity of the cursor position!
        val success = this.moveInternal(direction)
        this.modCount++
        return success
    }

    final override fun close() {
        if (!this.isOpen) {
            return
        }
        this.isOpen = false
        CursorUtils.executeCloseHandlers(this::closeInternal, this.closeHandlers)
    }

    final override fun onClose(action: CloseHandler): Cursor<K, V> {
        this.checkInvariants()
        this.closeHandlers += action
        return this
    }

    protected open fun checkInvariants() {
        if (!this.isOpen) {
            throw IllegalStateException("This cursor has already been closed: ${this}")
        }
    }

    // =================================================================================================================
    // ABSTRACT METHOD DECLARATIONS
    // =================================================================================================================

    protected abstract val keyOrNullInternal: K?

    protected abstract val valueOrNullInternal: V?

    protected abstract fun closeInternal()

    protected abstract fun firstInternal(): Boolean

    protected abstract fun lastInternal(): Boolean

    protected abstract fun moveInternal(direction: Order): Boolean

    protected abstract fun seekExactlyOrNextInternal(key: K): Boolean

    protected abstract fun seekExactlyOrPreviousInternal(key: K): Boolean
}