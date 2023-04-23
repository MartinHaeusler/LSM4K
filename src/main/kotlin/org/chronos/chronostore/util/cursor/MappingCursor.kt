package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.util.Order

class MappingCursor<C : Cursor<K, V>, K, V, MK, MV>(
    original: C,
    val mapKey: (K) -> MK,
    val mapKeyInverse: (MK) -> K,
    val mapValue: (V) -> MV,
) : WrappingCursor<C, MK, MV>(original) {

    override fun doFirst(): Boolean {
        return this.innerCursor.first()
    }

    override fun doLast(): Boolean {
        return this.innerCursor.last()
    }

    override fun doMove(direction: Order): Boolean {
        return this.innerCursor.move(direction)
    }

    override val keyOrNullInternal: MK?
        get() = this.innerCursor.keyOrNull?.let(mapKey)

    override val valueOrNullInternal: MV?
        get() = this.innerCursor.valueOrNull?.let(mapValue)

    override fun doSeekExactlyOrNext(key: MK): Boolean {
        return this.innerCursor.seekExactlyOrNext(mapKeyInverse(key))
    }

    override fun doSeekExactlyOrPrevious(key: MK): Boolean {
        return this.innerCursor.seekExactlyOrPrevious(mapKeyInverse(key))
    }

    override fun toString(): String {
        return "MappingCursor[${this.innerCursor}]"
    }

}