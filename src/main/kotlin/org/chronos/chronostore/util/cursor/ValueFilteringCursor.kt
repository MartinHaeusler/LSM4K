package org.chronos.chronostore.util.cursor

open class ValueFilteringCursor<C : Cursor<K, V>, K, V>(
    cursor: C,
    private val filterValue: (V?) -> Boolean,
) : FilteringCursor<C, K, V>(cursor) {

    override val isInnerPositionMatchingTheFilter: Boolean
        get() = this.filterValue(innerCursor.valueOrNull)

    override fun toString(): String {
        return "ValueFilteringCursor[${this.innerCursor}]"
    }

}