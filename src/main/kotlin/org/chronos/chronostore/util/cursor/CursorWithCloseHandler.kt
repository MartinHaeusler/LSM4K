package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.util.Order

open class CursorWithCloseHandler<C : Cursor<K, V>, K, V>(
    innerCursor: C,
    private val onClose: () -> Unit,
) : WrappingCursor<C, K, V>(innerCursor) {

    override fun doFirst(): Boolean {
        return this.innerCursor.first()
    }

    override fun doLast(): Boolean {
        return this.innerCursor.last()
    }

    override fun doMove(direction: Order): Boolean {
        return this.innerCursor.move(direction)
    }

    override fun doSeekExactlyOrNext(key: K): Boolean {
        return this.innerCursor.seekExactlyOrNext(key)
    }

    override fun doSeekExactlyOrPrevious(key: K): Boolean {
        return this.innerCursor.seekExactlyOrPrevious(key)
    }

    override fun peekNext(): Pair<K, V>? {
        return this.innerCursor.peekNext()
    }

    override fun peekPrevious(): Pair<K, V>? {
        return this.innerCursor.peekPrevious()
    }

    override val keyOrNullInternal: K?
        get() = this.innerCursor.keyOrNull

    override val valueOrNullInternal: V?
        get() = this.innerCursor.valueOrNull

    override fun closeInternal() {
        super.closeInternal()
        this.onClose()
    }

    override fun toString(): String {
        return "CursorWithCloseHandler[${this.innerCursor}]"
    }

}