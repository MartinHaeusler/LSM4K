package io.github.martinhaeusler.lsm4k.util.cursor

import io.github.martinhaeusler.lsm4k.util.cursor.CursorUtils.checkIsOpen

class BoundaryCheckingCursor<K : Comparable<*>, V>(
    private val innerCursor: CursorInternal<K, V>,
) : CursorInternal<K, V> {

    override var parent: CursorInternal<*, *>? = null
        set(value) {
            if (field === value) {
                return
            }
            check(field == null) {
                "Cannot assign another parent to this cursor; a parent is already present. Existing parent: ${field}, proposed new parent: ${value}"
            }
            field = value
        }

    override var isOpen: Boolean = true

    private val closeHandlers = mutableListOf<CloseHandler>()

    private var mayHaveNext: Boolean = true
    private var mayHavePrevious: Boolean = true

    override val keyOrNull: K?
        get() {
            this.checkIsOpen()
            return this.innerCursor.keyOrNull
        }

    override val valueOrNull: V?
        get() {
            this.checkIsOpen()
            return this.innerCursor.valueOrNull
        }

    override val isValidPosition: Boolean
        get() {
            this.checkIsOpen()
            return this.innerCursor.isValidPosition
        }

    init {
        this.innerCursor.parent = this
    }

    override fun invalidatePositionInternal() {
        this.checkIsOpen()
        this.innerCursor.invalidatePositionInternal()
        this.mayHaveNext = false
        this.mayHavePrevious = false
    }

    override fun firstInternal(): Boolean {
        this.checkIsOpen()
        val result = this.innerCursor.firstInternal()
        // if the move succeeded, there may be a next element
        // if the move failed, the position is invalid anyway
        this.mayHaveNext = result
        // at first position, there's NEVER a previous.
        this.mayHavePrevious = false
        return result
    }

    override fun lastInternal(): Boolean {
        this.checkIsOpen()
        val result = this.innerCursor.lastInternal()
        // if the move succeeded, there may be a next element
        // if the move failed, the position is invalid anyway
        this.mayHavePrevious = result
        // at first position, there's NEVER a previous.
        this.mayHaveNext = false
        return result
    }

    override fun nextInternal(): Boolean {
        this.checkIsOpen()
        return if (!this.mayHaveNext) {
            // don't bother the cursor
            false
        } else {
            val result = this.innerCursor.nextInternal()
            // if the move to next failed, there is no next element
            // if the move to next succeeded, there may be another next element.
            this.mayHaveNext = result
            result
        }
    }

    override fun previousInternal(): Boolean {
        this.checkIsOpen()
        return if (!this.mayHavePrevious) {
            // don't bother the cursor
            false
        } else {
            val result = this.innerCursor.previousInternal()
            // if the move to previous failed, there is no previous element
            // if the move to previous succeeded, there may be another previous element.
            this.mayHavePrevious = result
            result
        }
    }

    override fun seekExactlyOrNextInternal(key: K): Boolean {
        this.checkIsOpen()
        val result = this.innerCursor.seekExactlyOrNextInternal(key)
        this.mayHaveNext = result
        this.mayHavePrevious = result
        return result
    }

    override fun seekExactlyOrPreviousInternal(key: K): Boolean {
        this.checkIsOpen()
        val result = this.innerCursor.seekExactlyOrPreviousInternal(key)
        this.mayHaveNext = result
        this.mayHavePrevious = result
        return result
    }

    override fun onClose(action: CloseHandler): Cursor<K, V> {
        this.checkIsOpen()
        this.closeHandlers += action
        return this
    }

    override fun closeInternal() {
        if (!this.isOpen) {
            return
        }
        this.isOpen = false
        CursorUtils.executeCloseHandlers(this.innerCursor::closeInternal, this.closeHandlers)
    }

}