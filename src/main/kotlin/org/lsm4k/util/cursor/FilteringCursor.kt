package org.lsm4k.util.cursor

import org.lsm4k.util.cursor.CursorUtils.checkIsOpen

class FilteringCursor<K, V>(
    private val innerCursor: CursorInternal<K, V>,
    private val filter: EntryFilter<K, V>,
) : CursorInternal<K, V> {

    override var parent: CursorInternal<*, *>? = null
        set(value) {
            if (field === value) {
                return
            }
            check(field == null) {
                "Cannot assign another parent to this cursor; a parent is already present." +
                    " Existing parent: ${field}, proposed new parent: ${value}"
            }
            field = value
        }

    override val isOpen: Boolean
        get() = this.innerCursor.isOpen

    override val keyOrNull: K?
        get() {
            this.checkIsOpen()
            // we only ever stop the inner cursor at locations which
            // match the filter. So we can safely pass the inner key directly.
            return this.innerCursor.keyOrNull
        }

    override val valueOrNull: V?
        get() {
            this.checkIsOpen()
            // we only ever stop the inner cursor at locations which
            // match the filter. So we can safely pass the inner value directly.
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
    }

    override fun firstInternal(): Boolean {
        this.checkIsOpen()
        return if (this.innerCursor.firstInternal()) {
            if (this.isInnerCursorMatchingTheFilter()) {
                true
            } else {
                this.moveNextUntilFilterIsMatched(returnToInitialPositionIfNotFound = false)
            }
        } else {
            false
        }
    }

    override fun lastInternal(): Boolean {
        this.checkIsOpen()
        return if (this.innerCursor.lastInternal()) {
            if (this.isInnerCursorMatchingTheFilter()) {
                true
            } else {
                this.movePreviousUntilFilterIsMatched(returnToInitialPositionIfNotFound = false)
            }
        } else {
            false
        }
    }

    override fun nextInternal(): Boolean {
        this.checkIsOpen()
        return this.moveNextUntilFilterIsMatched(returnToInitialPositionIfNotFound = true)
    }

    override fun previousInternal(): Boolean {
        this.checkIsOpen()
        return this.movePreviousUntilFilterIsMatched(returnToInitialPositionIfNotFound = true)
    }

    override fun seekExactlyOrPreviousInternal(key: K): Boolean {
        this.checkIsOpen()
        return if (!this.innerCursor.seekExactlyOrPreviousInternal(key)) {
            // inner cursor found nothing
            false
        } else if (this.isInnerCursorMatchingTheFilter()) {
            // inner cursor found something that matches our filter -> ok
            true
        } else {
            // inner cursor found something, but it didn't match our filter,
            // move to the previous element until we either find a key which
            // matches the filter, or run out of keys
            this.movePreviousUntilFilterIsMatched(returnToInitialPositionIfNotFound = false)
        }
    }

    override fun seekExactlyOrNextInternal(key: K): Boolean {
        this.checkIsOpen()
        return if (!this.innerCursor.seekExactlyOrNextInternal(key)) {
            // inner cursor found nothing
            false
        } else if (this.isInnerCursorMatchingTheFilter()) {
            // inner cursor found something that matches our filter -> ok
            true
        } else {
            // inner cursor found something, but it didn't match our filter,
            // move to the next element until we either find a key which
            // matches the filter, or run out of keys
            this.moveNextUntilFilterIsMatched(returnToInitialPositionIfNotFound = false)
        }
    }

    override fun closeInternal() {
        this.innerCursor.closeInternal()
    }

    override fun onClose(action: CloseHandler): Cursor<K, V> {
        this.checkIsOpen()
        this.innerCursor.onClose(action)
        return this
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private fun isInnerCursorMatchingTheFilter(): Boolean {
        return this.filter.filter(
            key = this.innerCursor.key,
            value = this.innerCursor.value,
        )
    }

    private fun moveNextUntilFilterIsMatched(returnToInitialPositionIfNotFound: Boolean): Boolean {
        // we might need to return to our current position, so keep track of how many steps we took.
        var moves = 0
        do {
            if (!this.innerCursor.nextInternal()) {
                // inner cursor is exhausted. Do we need to return to the initial position?
                if (returnToInitialPositionIfNotFound) {
                    repeat(moves) {
                        this.innerCursor.previousInternal()
                    }
                }
                return false
            } else {
                moves++
            }
        } while (!this.isInnerCursorMatchingTheFilter())
        return true
    }

    private fun movePreviousUntilFilterIsMatched(returnToInitialPositionIfNotFound: Boolean): Boolean {
        // we might need to return to our current position, so keep track of how many steps we took.
        var moves = 0
        do {
            if (!this.innerCursor.previousInternal()) {
                // inner cursor is exhausted. Do we need to return to the initial position?
                if (returnToInitialPositionIfNotFound) {
                    repeat(moves) {
                        this.innerCursor.nextInternal()
                    }
                }
                return false
            } else {
                moves++
            }
        } while (!this.isInnerCursorMatchingTheFilter())
        return true
    }

    fun interface EntryFilter<K, V> {

        fun filter(key: K, value: V): Boolean

    }
}