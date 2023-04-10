package org.chronos.chronostore.util.cursor

class EmptyCursor<K, V>(
    val getCursorName: () -> String
): Cursor<K, V> {


    override var modCount: Long = 0

    override var isOpen: Boolean = true


    override val isValidPosition: Boolean
        get() = false

    override fun invalidatePosition() {
        check(this.isOpen, ::createAlreadyClosedMessage)
        this.modCount++
    }

    override fun first(): Boolean {
        check(this.isOpen, ::createAlreadyClosedMessage)
        this.modCount++
        return false
    }

    override fun last(): Boolean {
        check(this.isOpen, ::createAlreadyClosedMessage)
        this.modCount++
        return false
    }

    override fun next(): Boolean {
        check(this.isOpen, ::createAlreadyClosedMessage)
        this.modCount++
        return false
    }

    override fun previous(): Boolean {
        check(this.isOpen, ::createAlreadyClosedMessage)
        this.modCount++
        return false
    }

    override val keyOrNull: K?
        get() {
            check(this.isOpen, ::createAlreadyClosedMessage)
            return null
        }

    override val valueOrNull: V?
        get() {
            check(this.isOpen, ::createAlreadyClosedMessage)
            return null
        }

    override fun close() {
        this.isOpen = false
    }

    override fun seekExactlyOrNext(key: K): Boolean {
        check(this.isOpen, ::createAlreadyClosedMessage)
        return false
    }

    override fun seekExactlyOrPrevious(key: K): Boolean {
        check(this.isOpen, ::createAlreadyClosedMessage)
        return false
    }

    private fun createAlreadyClosedMessage(): String {
        return "This cursor on ${this.getCursorName()} has already been closed!"
    }

}