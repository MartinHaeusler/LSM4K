package org.chronos.chronostore.util.cursor

open class KeyFilteringCursor<C : Cursor<K, V>, K, V>(
    cursor: C,
    private val filterKey: (K) -> Boolean,
) : FilteringCursor<C, K, V>(cursor) {

    override val isInnerPositionMatchingTheFilter: Boolean
        get() = this.filterKey(innerCursor.key)

    override fun toString(): String {
        return "KeyFilteringCursor[${this.innerCursor}]"
    }
}