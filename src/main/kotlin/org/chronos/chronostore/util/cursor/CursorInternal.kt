package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.util.cursor.CursorUtils.checkNoParent

/**
 * Internal representation of a [Cursor] which offers an internal API.
 *
 * The internal API should be used by the [parent] cursor and consists of:
 *
 * - [invalidatePositionInternal] (mirrors public method [invalidatePosition])
 * - [firstInternal] (mirrors public method [first])
 * - [lastInternal] (mirrors public method [last])
 * - [nextInternal] (mirrors public method [next])
 * - [previousInternal] (mirrors public method [previous])
 * - [seekExactlyOrNextInternal] (mirrors public method [seekExactlyOrNext])
 * - [seekExactlyOrPreviousInternal] (mirrors public method [seekExactlyOrPrevious])
 *
 * The default implementations of all public methods forward to their internal
 * counterparts if no [parent] cursor is assigned. If a [parent] cursor is assigned,
 * they will throw an [IllegalStateException], as the public API should not be used
 */
interface CursorInternal<K, V> : Cursor<K, V> {

    /**
     * The parent of this cursor, if any.
     *
     * If there is a parent cursor, all public API methods defined in [Cursor] will
     * throw an [IllegalStateException], as ownership of this cursor has been passed
     * onto the parent cursor.
     *
     * **Implementation Note:**
     *
     * Implementations should only allow to set this property to a non-`null` value
     * **once**. Once a parent has been assigned, it should not be possible to remove
     * it again, nor should it be allowed to assign a different (w.r.t. `===`) parent.
     */
    var parent: CursorInternal<*, *>?

    override fun invalidatePosition() {
        this.checkNoParent()
        this.invalidatePositionInternal()
    }

    override fun first(): Boolean {
        this.checkNoParent()
        return this.firstInternal()
    }

    override fun last(): Boolean {
        this.checkNoParent()
        return this.lastInternal()
    }

    override fun next(): Boolean {
        this.checkNoParent()
        return this.nextInternal()
    }

    override fun previous(): Boolean {
        this.checkNoParent()
        return this.previousInternal()
    }

    override fun seekExactlyOrNext(key: K): Boolean {
        this.checkNoParent()
        return this.seekExactlyOrNextInternal(key)
    }

    override fun seekExactlyOrPrevious(key: K): Boolean {
        this.checkNoParent()
        return this.seekExactlyOrPreviousInternal(key)
    }

    override fun peekNext(): Pair<K, V>? {
        this.checkNoParent()
        return this.peekNextInternal()
    }

    override fun peekPrevious(): Pair<K, V>? {
        this.checkNoParent()
        return this.peekPreviousInternal()
    }

    /**
     * Same as [invalidatePosition], but can be called from the [parent] cursor.
     *
     * @see invalidatePosition
     */
    fun invalidatePositionInternal()

    /**
     * Same as [first], but can be called from the [parent] cursor.
     *
     * @see first
     */
    fun firstInternal(): Boolean

    /**
     * Same as [last], but can be called from the [parent] cursor.
     *
     * @see last
     */
    fun lastInternal(): Boolean

    /**
     * Same as [next], but can be called from the [parent] cursor.
     *
     * @see next
     */
    fun nextInternal(): Boolean

    /**
     * Same as [previous], but can be called from the [parent] cursor.
     *
     * @see previous
     */
    fun previousInternal(): Boolean

    /**
     * Same as [seekExactlyOrNext], but can be called from the [parent] cursor.
     *
     * @see seekExactlyOrNext
     */
    fun seekExactlyOrNextInternal(key: K): Boolean

    /**
     * Same as [seekExactlyOrPrevious], but can be called from the [parent] cursor.
     *
     * @see seekExactlyOrPrevious
     */
    fun seekExactlyOrPreviousInternal(key: K): Boolean

    fun peekPreviousInternal(): Pair<K, V>? {
        if (!this.isValidPosition) {
            return null
        }
        if (!this.previousInternal()) {
            return null
        }
        val entry = this.key to this.value
        check(this.nextInternal()) {
            "Illegal Iterator state - move 'next()' failed!"
        }
        return entry
    }

    fun peekNextInternal(): Pair<K, V>? {
        if (!this.isValidPosition) {
            return null
        }
        if (!this.nextInternal()) {
            return null
        }
        val entry = this.key to this.value

        check(this.previousInternal()) {
            "Illegal Iterator state - move 'previous()' failed!"
        }
        return entry
    }

    override fun close() {
        if (this.parent != null) {
            // ignore the close() call; the parent cursor
            // is responsible for calling closeInternal().
            return
        }
        if (!this.isOpen) {
            return
        }
        this.closeInternal()
    }

    /**
     * Same as [close], but can be called from the [parent] cursor.
     *
     * @see [close]
     */
    fun closeInternal()

}