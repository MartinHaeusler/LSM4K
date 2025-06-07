package io.github.martinhaeusler.lsm4k.util.cursor

import io.github.martinhaeusler.lsm4k.util.cursor.CursorUtils.checkIsOpen

class IndexBasedCursor<K : Comparable<K>, V>(
    private val minIndex: Int,
    private val maxIndex: Int,
    private val getEntryAtIndex: (Int) -> Pair<K, V>,
    val name: String,
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

    private var currentIndex = this.minIndex - 1

    override var isOpen: Boolean = true

    private var currentEntry: Pair<K, V>? = null

    private val closeHandlers = mutableListOf<CloseHandler>()

    override val keyOrNull: K?
        get() {
            this.checkIsOpen()
            if (!this.isValidPosition) {
                return null
            }
            val entry = this.currentEntry
                ?: let { this.getEntryAtIndex(this.currentIndex) }
            this.currentEntry = entry
            return entry.first
        }

    override val valueOrNull: V?
        get() {
            this.checkIsOpen()
            if (!this.isValidPosition) {
                return null
            }
            val entry = this.currentEntry
                ?: let { this.getEntryAtIndex(this.currentIndex) }
            this.currentEntry = entry
            return entry.second
        }

    init {
        require(minIndex >= 0) { "Argument 'minIndex' (${minIndex}) must not be negative!" }
        require(maxIndex >= 0) { "Argument 'maxIndex' (${maxIndex}) must not be negative!" }
        require(minIndex <= maxIndex) { "Argument 'minIndex' (${minIndex}) must not be greater than argument 'maxIndex' (${maxIndex})!" }
    }

    override val isValidPosition: Boolean
        get() = this.currentIndex >= minIndex

    override fun invalidatePositionInternal() {
        this.checkIsOpen()
        this.currentIndex = -1
    }

    override fun firstInternal(): Boolean {
        this.checkIsOpen()
        this.currentIndex = minIndex
        this.currentEntry = null
        return true
    }

    override fun lastInternal(): Boolean {
        this.checkIsOpen()
        this.currentIndex = this.maxIndex
        this.currentEntry = null
        return true
    }

    override fun nextInternal(): Boolean {
        this.checkIsOpen()
        if (!this.isValidPosition) {
            return false
        }
        if (this.currentIndex >= this.maxIndex) {
            return false
        }
        this.currentIndex += 1
        this.currentEntry = null
        return true
    }

    override fun previousInternal(): Boolean {
        this.checkIsOpen()
        if (!this.isValidPosition) {
            return false
        }
        if (this.currentIndex <= 0) {
            return false
        }
        this.currentIndex -= 1
        this.currentEntry = null
        return true
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
        CursorUtils.executeCloseHandlers(this.closeHandlers)
    }

    override fun seekExactlyOrNextInternal(key: K): Boolean {
        this.checkIsOpen()
        if (key == this.keyOrNull) {
            // we're already there
            return true
        }
        if (!this.firstInternal()) {
            return false
        }
        while (this.key < key) {
            if (!this.nextInternal()) {
                this.invalidatePositionInternal()
                return false
            }
        }
        return true
    }

    override fun seekExactlyOrPreviousInternal(key: K): Boolean {
        this.checkIsOpen()
        if (key == this.keyOrNull) {
            // we're already there
            return true
        }
        if (!this.lastInternal()) {
            return false
        }
        while (this.key > key) {
            if (!this.previousInternal()) {
                this.invalidatePositionInternal()
                return false
            }
        }
        return true
    }

    override fun toString(): String {
        return "IndexBasedCursor[${this.name}]"
    }
}