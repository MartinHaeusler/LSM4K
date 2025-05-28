package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.util.Order
import org.chronos.chronostore.util.Order.ASCENDING
import org.chronos.chronostore.util.Order.DESCENDING
import java.util.*
import java.util.concurrent.ConcurrentSkipListSet

class OverlayCursor<K : Comparable<K>, V>(
    /** An ordered list of cursors. The first has the lowest priority, the last has the highest priority. */
    orderedCursors: List<Cursor<K, V?>>,
) : Cursor<K, V> {

    companion object {

        @Suppress("UNCHECKED_CAST")
        @JvmName("overlayOntoNonNullable")
        fun <K : Comparable<K>, V> Cursor<K, V>.overlayOnto(base: Cursor<K, V>): Cursor<K, V> {
            return OverlayCursor(base as Cursor<K, V?>, this as Cursor<K, V?>)
        }

        @Suppress("UNCHECKED_CAST")
        @JvmName("overlayOntoNullable")
        fun <K : Comparable<K>, V> Cursor<K, V?>.overlayOnto(base: Cursor<K, V>): Cursor<K, V> {
            return OverlayCursor(base as Cursor<K, V?>, this)
        }

        @Suppress("UNCHECKED_CAST")
        fun <K : Comparable<K>, V> Cursor<K, V>.overlayUnder(overlay: Cursor<K, V?>): Cursor<K, V> {
            return OverlayCursor(this as Cursor<K, V?>, overlay)
        }

    }

    private val cursorsByPriority: List<Cursor<K, V?>> = orderedCursors

    private val cursorToPriority = IdentityHashMap<Cursor<K, V?>, Int>()

    private var cursorsByKey: NavigableSet<Cursor<K, V?>>

    private var currentDirection = ASCENDING

    override var modCount: Long = 0

    override var isOpen: Boolean = true

    override var isValidPosition: Boolean = false

    private val closeHandlers = mutableListOf<CloseHandler>()

    override val keyOrNull: K?
        get() {
            this.assertNotClosed()
            return this.keyOrNullInternal
        }

    override val valueOrNull: V?
        get() {
            this.assertNotClosed()
            return this.valueOrNullInternal
        }

    private var keyOrNullInternal: K? = null
    private var valueOrNullInternal: V? = null

    constructor(vararg orderedCursors: Cursor<K, V?>) : this(listOf(*orderedCursors))

    init {
        var priority = 0
        for (cursor in this.cursorsByPriority) {
            this.cursorToPriority[cursor] = priority
            priority++
        }
        this.cursorsByKey = this.createCursorsByKeyMapForDirection(currentDirection)
        for (cursor in this.cursorsByPriority) {
            this.cursorsByKey += cursor
        }
        this.invalidatePosition()
    }

    override fun invalidatePosition() {
        assertNotClosed()
        for (cursor in this.cursorsByPriority) {
            cursor.invalidatePosition()
        }
        this.isValidPosition = false
    }

    override fun first(): Boolean {
        this.assertNotClosed()

        // reset all cursors to the start
        this.cursorsByKey = this.createCursorsByKeyMapForDirection(ASCENDING)
        for (cursor in this.cursorsByPriority) {
            if (cursor.first()) {
                this.cursorsByKey += cursor
            }
        }

        if (this.cursorsByKey.isEmpty()) {
            // none of our cursors is valid after calling "first()" -> they're all empty!
            this.invalidatePosition()
            return false
        }

        // remember that we're going in ascending direction
        this.currentDirection = ASCENDING

        return findValidNextHigherEntry()
    }

    override fun last(): Boolean {
        this.assertNotClosed()
        this.cursorsByKey = this.createCursorsByKeyMapForDirection(DESCENDING)
        for (cursor in this.cursorsByPriority) {
            if (cursor.last()) {
                this.cursorsByKey += cursor
            }
        }

        if (this.cursorsByKey.isEmpty()) {
            // none of our cursors is valid after calling "last()" -> they're all empty!
            this.invalidatePosition()
            return false
        }

        // remember that we're going in ascending direction
        this.currentDirection = DESCENDING

        return findValidNextLowerEntry()
    }

    private fun prepareForAscendingMove() {
        if (this.currentDirection == ASCENDING) {
            // we're already on the ascending track -> nothing to do
            return
        }
        // direction change -> reset the cursors to arrive at the regular state we would
        // have during ascending iteration.
        val currentKey = this.key
        this.cursorsByKey = this.createCursorsByKeyMapForDirection(ASCENDING)
        for (cursor in this.cursorsByPriority) {
            if (!cursor.isValidPosition) {
                if (cursor.seekExactlyOrNext(currentKey)) {
                    this.cursorsByKey += cursor
                }
            } else {
                while (cursor.key < currentKey) {
                    if (!cursor.next()) {
                        cursor.invalidatePosition()
                        break
                    }
                }
                if (cursor.isValidPosition) {
                    this.cursorsByKey += cursor
                }
            }
        }
    }

    private fun prepareForDescendingMove() {
        if (this.currentDirection == DESCENDING) {
            // we're already on the descending track -> nothing to do.
            return
        }
        // direction change -> reset the cursors to arrive at the regular state we would
        // have during descending iteration.
        val currentKey = this.key
        this.cursorsByKey = this.createCursorsByKeyMapForDirection(DESCENDING)
        for (cursor in this.cursorsByPriority) {
            if (!cursor.isValidPosition) {
                if (cursor.seekExactlyOrPrevious(currentKey)) {
                    this.cursorsByKey += cursor
                }
            } else {
                while (cursor.key > currentKey) {
                    if (!cursor.previous()) {
                        cursor.invalidatePosition()
                        break
                    }
                }
                if (cursor.isValidPosition) {
                    this.cursorsByKey += cursor
                }
            }
        }
    }

    private fun findValidNextLowerEntry(max: K? = null): Boolean {
        // did we change direction?
        this.prepareForDescendingMove()

        while (true) {
            // take the first key. The comparator asserts that we get the cursor
            // with the highest priority in case that there are multiple overrides for the same key.
            val maxPriorityCursor = this.cursorsByKey.first()
            val currentKey = maxPriorityCursor.key

            // check the cursor with the highest priority. Is the value a deletion?
            val value = maxPriorityCursor.valueOrNull
            if (value != null && (max == null || currentKey < max)) {
                // the value is not a deletion; we've found our valid position.
                this.currentDirection = DESCENDING
                markValidPosition(currentKey, value)
                return true
            } else {
                // the value is a deletion according to an overlay. Move to the next-lower value.
                while (this.cursorsByKey.isNotEmpty() && this.cursorsByKey.first().key >= currentKey) {
                    val cursor = this.cursorsByKey.removeFirst()
                    // fast-forward the cursor
                    while (cursor.key >= currentKey) {
                        if (!cursor.previous()) {
                            // don't add the cursor back into the tree, invalidate it
                            cursor.invalidatePosition()
                            break
                        }
                    }
                    if (cursor.isValidPosition) {
                        // re-add the cursor into the tree
                        this.cursorsByKey += cursor
                    }
                }
                // are there any valid cursors left?
                if (this.cursorsByKey.isEmpty()) {
                    this.currentDirection = DESCENDING
                    return false
                }

                // otherwise, retry with the next position
                continue
            }
        }
    }

    private fun findValidNextHigherEntry(min: K? = null): Boolean {
        // did we change direction?
        prepareForAscendingMove()

        while (true) {
            // take the first key. The comparator asserts that we get the cursor
            // with the highest priority in case that there are multiple overrides for the same key.
            val maxPriorityCursor = this.cursorsByKey.first()
            val currentKey = maxPriorityCursor.key

            // check the cursor with the highest priority. Is the value a deletion?
            val value = maxPriorityCursor.valueOrNull
            if (value != null && (min == null || currentKey > min)) {
                // the value is not a deletion; we've found our valid position.
                this.currentDirection = ASCENDING
                markValidPosition(currentKey, value)
                return true
            } else {
                // the value is a deletion according to an overlay. Move to the next-higher value.
                while (this.cursorsByKey.isNotEmpty() && this.cursorsByKey.first().key <= currentKey) {
                    val cursor = this.cursorsByKey.removeFirst()
                    // fast-forward the cursor
                    while (cursor.key <= currentKey) {
                        if (!cursor.next()) {
                            // don't add the cursor back into the tree, invalidate it
                            cursor.invalidatePosition()
                            break
                        }
                    }
                    if (cursor.isValidPosition) {
                        // re-add the cursor into the tree
                        if (!this.cursorsByKey.add(cursor)) {
                            error("Cursor was already in the set!")
                        }
                    }
                }
                // are there any valid cursors left?
                if (this.cursorsByKey.isEmpty()) {
                    this.currentDirection = ASCENDING
                    return false
                }

                // otherwise, retry with the next position
                continue
            }
        }
    }

    private fun markValidPosition(currentKey: K, value: V) {
        this.keyOrNullInternal = currentKey
        this.valueOrNullInternal = value
        this.isValidPosition = true
    }

    override fun next(): Boolean {
        this.assertNotClosed()
        if (!this.isValidPosition) {
            return false
        }

        return this.findValidNextHigherEntry(this.key)
    }

    override fun previous(): Boolean {
        this.assertNotClosed()
        if (!this.isValidPosition) {
            return false
        }
        return this.findValidNextLowerEntry(this.key)
    }

    override fun seekExactlyOrNext(key: K): Boolean {
        this.assertNotClosed()
        // reset all cursors to the start
        this.cursorsByKey = this.createCursorsByKeyMapForDirection(ASCENDING)
        for (cursor in this.cursorsByPriority) {
            if (cursor.seekExactlyOrNext(key)) {
                this.cursorsByKey += cursor
            }
        }

        if (this.cursorsByKey.isEmpty()) {
            // none of our cursors is valid after calling "seekExactlyOrNext()" -> no cursor has any data
            this.invalidatePosition()
            return false
        }

        // remember that we're going in ascending direction
        this.currentDirection = ASCENDING

        val found = this.findValidNextHigherEntry()
        if (!found) {
            this.invalidatePosition()
        }
        return found
    }

    override fun seekExactlyOrPrevious(key: K): Boolean {
        this.assertNotClosed()
        // reset all cursors to the start
        this.cursorsByPriority.forEach { it.seekExactlyOrPrevious(key) }
        // order has changed, re-insert the non-empty ones into the tree
        // (we don't care about the invalid ones, they point to empty keyspaces)
        this.cursorsByKey = this.createCursorsByKeyMapForDirection(DESCENDING)
        for (cursor in this.cursorsByPriority) {
            if (cursor.isValidPosition) {
                this.cursorsByKey += cursor
            }
        }

        if (this.cursorsByKey.isEmpty()) {
            // none of our cursors is valid after calling "seekExactlyOrPrevious()" -> no cursor has any data
            this.invalidatePosition()
            return false
        }

        // remember that we're going in descending direction
        this.currentDirection = DESCENDING

        val found = this.findValidNextLowerEntry()
        if (!found) {
            this.invalidatePosition()
        }
        return found
    }


    override fun onClose(action: CloseHandler): Cursor<K, V> {
        this.assertNotClosed()
        this.closeHandlers += action
        return this
    }

    override fun close() {
        if (!this.isOpen) {
            return
        }
        this.isOpen = false
        CursorUtils.executeCloseHandlers(this.closeHandlers)
        for (cursor in this.cursorsByPriority) {
            cursor.close()
        }
    }

    private fun assertNotClosed() {
        check(this.isOpen) {
            "This cursor has already been closed!"
        }
    }

    private fun createCursorsByKeyMapForDirection(direction: Order): NavigableSet<Cursor<K, V?>> {
        return ConcurrentSkipListSet(
            CursorByKeyAndPriorityComparator(
                direction = direction,
                cursorToPriority = cursorToPriority,
            )
        )
    }

    private fun Sequence<Cursor<K, V?>>.maxByPriorityOrThrow(): Cursor<K, V?> {
        return this.maxByPriorityOrNull()
            ?: error("Could not determine cursor with highest priority: the sequence is empty!")
    }

    private fun Sequence<Cursor<K, V?>>.maxByPriorityOrNull(): Cursor<K, V?>? {
        return this.maxByOrNull { getCursorPriority(it) }
    }

    private fun getCursorPriority(cursor: Cursor<K, V?>): Int {
        return this.cursorToPriority[cursor]
            ?: error("Cursor is not a child of this overlay cursor: ${cursor}")
    }

    override fun toString(): String {
        return "OverlayCursor[${this.cursorsByPriority.joinToString()}]"
    }

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    /**
     * Compares cursors by their current [key][Cursor.key] and tie-breaks by the [priority][cursorToPriority].
     *
     * If multiple cursors have the same [key][Cursor.key], this comparator sorts by the cursor's
     * [priority][cursorToPriority] (descending). If the key ordering is ascending or descending
     * depends on the [direction].
     */
    private class CursorByKeyAndPriorityComparator<K : Comparable<K>, V>(
        private val direction: Order,
        private val cursorToPriority: IdentityHashMap<Cursor<K, V?>, Int>,
    ) : Comparator<Cursor<K, V?>> {

        override fun compare(o1: Cursor<K, V?>?, o2: Cursor<K, V?>?): Int {
            if (o1 == null && o2 == null) {
                return 0
            } else if (o1 != null && o2 == null) {
                return -1
            } else if (o1 == null && o2 != null) {
                return +1
            }
            o1!!
            o2!!

            val k1 = o1.keyOrNull
            val k2 = o2.keyOrNull

            if (k1 == null && k2 == null) {
                return 0
            } else if (k1 != null && k2 == null) {
                return -1
            } else if (k1 == null && k2 != null) {
                return +1
            }

            k1!!
            k2!!
            val keyCompare = k1.compareTo(k2)
            if (keyCompare != 0) {
                return direction.applyToCompare(keyCompare)
            }

            // final fallback: compare by comparator priority (DESCENDING!)
            val p1 = this.getCursorPriority(o1)
            val p2 = this.getCursorPriority(o2)
            // multiply the comparison by -1 to get descending sorting
            return p1.compareTo(p2) * -1
        }

        private fun getCursorPriority(cursor: Cursor<K, V?>): Int {
            return this.cursorToPriority[cursor]
                ?: error("Cursor is not a child of this overlay cursor: ${cursor}")
        }
    }
}