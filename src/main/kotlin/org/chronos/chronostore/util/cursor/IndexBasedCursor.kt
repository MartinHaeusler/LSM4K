package org.chronos.chronostore.util.cursor

class IndexBasedCursor<K : Comparable<K>, V>(
    val minIndex: Int,
    val maxIndex: Int,
    val getEntryAtIndex: (Int) -> Pair<K, V>,
    val getCursorName: () -> String,
) : Cursor<K, V> {

    private var currentIndex = this.minIndex - 1

    override var modCount: Long = 0

    override var isOpen: Boolean = true

    private var currentEntry: Pair<K, V>? = null

    private val closeHandlers = mutableListOf<CloseHandler>()

    init {
        require(minIndex >= 0) { "Argument 'minIndex' (${minIndex}) must not be negative!" }
        require(maxIndex >= 0) { "Argument 'maxIndex' (${maxIndex}) must not be negative!" }
        require(minIndex <= maxIndex) { "Argument 'minIndex' (${minIndex}) must not be greater than argument 'maxIndex' (${maxIndex})!" }
    }

    override val isValidPosition: Boolean
        get() = this.currentIndex >= minIndex

    override fun invalidatePosition() {
        check(this.isOpen, ::createAlreadyClosedMessage)
        this.currentIndex = -1
        this.modCount++
    }

    override fun first(): Boolean {
        check(this.isOpen, ::createAlreadyClosedMessage)
        this.currentIndex = minIndex
        this.currentEntry = null
        this.modCount++
        return true
    }

    override fun last(): Boolean {
        check(this.isOpen, ::createAlreadyClosedMessage)
        this.currentIndex = this.maxIndex
        this.currentEntry = null
        this.modCount++
        return true
    }

    override fun next(): Boolean {
        check(this.isOpen, ::createAlreadyClosedMessage)
        this.modCount++
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

    override fun previous(): Boolean {
        check(this.isOpen, ::createAlreadyClosedMessage)
        this.modCount++
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

    override val keyOrNull: K?
        get() {
            check(this.isOpen, ::createAlreadyClosedMessage)
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
            check(this.isOpen, ::createAlreadyClosedMessage)
            if (!this.isValidPosition) {
                return null
            }
            val entry = this.currentEntry
                ?: let { this.getEntryAtIndex(this.currentIndex) }
            this.currentEntry = entry
            return entry.second
        }

    override fun onClose(action: CloseHandler): Cursor<K, V> {
        check(this.isOpen, ::createAlreadyClosedMessage)
        this.closeHandlers += action
        return this
    }

    override fun close() {
        this.isOpen = false
        CursorUtils.executeCloseHandlers(this.closeHandlers)
    }

    override fun seekExactlyOrNext(key: K): Boolean {
        check(this.isOpen, ::createAlreadyClosedMessage)
        if (key == this.keyOrNull) {
            // we're already there
            return true
        }
        if (!this.first()) {
            return false
        }
        while (this.key < key) {
            if (!this.next()) {
                this.invalidatePosition()
                return false
            }
        }
        return true
    }

    override fun seekExactlyOrPrevious(key: K): Boolean {
        check(this.isOpen, ::createAlreadyClosedMessage)
        if (key == this.keyOrNull) {
            // we're already there
            return true
        }
        if (!this.last()) {
            return false
        }
        while (this.key > key) {
            if (!this.previous()) {
                this.invalidatePosition()
                return false
            }
        }
        return true
    }

    private fun createAlreadyClosedMessage(): String {
        return "This cursor on ${this.getCursorName()} has already been closed!"
    }

    override fun toString(): String {
        return "IndexBasedCursor[${this.getCursorName}]"
    }
}