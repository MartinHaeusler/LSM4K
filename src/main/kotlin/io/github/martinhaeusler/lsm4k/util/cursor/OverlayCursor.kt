package io.github.martinhaeusler.lsm4k.util.cursor

import io.github.martinhaeusler.lsm4k.util.Order
import io.github.martinhaeusler.lsm4k.util.Order.ASCENDING
import io.github.martinhaeusler.lsm4k.util.Order.DESCENDING
import io.github.martinhaeusler.lsm4k.util.cursor.CursorUtils.checkIsOpen
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsReporter
import java.util.*
import java.util.concurrent.ConcurrentSkipListSet

class OverlayCursor<K : Comparable<K>, V>(
    /** An ordered list of cursors. The first has the lowest priority, the last has the highest priority. */
    orderedCursors: List<CursorInternal<K, V?>>,
    private val statisticsReporter: StatisticsReporter,
) : CursorInternal<K, V> {

    companion object {

        const val CURSOR_NAME = "OverlayCursor"

        @Suppress("UNCHECKED_CAST")
        @JvmName("overlayOntoNonNullable")
        fun <K : Comparable<K>, V> Cursor<K, V>.overlayOnto(base: Cursor<K, V>, statisticsReporter: StatisticsReporter): Cursor<K, V> {
            return OverlayCursor(statisticsReporter, base as CursorInternal<K, V?>, this as CursorInternal<K, V?>)
        }

        @Suppress("UNCHECKED_CAST")
        @JvmName("overlayOntoNullable")
        fun <K : Comparable<K>, V> Cursor<K, V?>.overlayOnto(base: Cursor<K, V>, statisticsReporter: StatisticsReporter): Cursor<K, V> {
            return OverlayCursor(statisticsReporter, base as CursorInternal<K, V?>, this as CursorInternal<K, V?>)
        }

        @Suppress("UNCHECKED_CAST")
        fun <K : Comparable<K>, V> Cursor<K, V>.overlayUnder(overlay: Cursor<K, V?>, statisticsReporter: StatisticsReporter): Cursor<K, V> {
            return OverlayCursor(statisticsReporter, this as CursorInternal<K, V?>, overlay as CursorInternal<K, V?>)
        }

    }

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

    private val cursorsByPriority: List<CursorInternal<K, V?>> = orderedCursors

    private val cursorToPriority = IdentityHashMap<CursorInternal<K, V?>, Int>()

    private var cursorsByKey: NavigableSet<CursorInternal<K, V?>>

    private var currentDirection = ASCENDING

    override var isOpen: Boolean = true

    override var isValidPosition: Boolean = false

    private val closeHandlers = mutableListOf<CloseHandler>()

    override val keyOrNull: K?
        get() {
            this.checkIsOpen()
            return this.keyOrNullInternal
        }

    override val valueOrNull: V?
        get() {
            this.checkIsOpen()
            return this.valueOrNullInternal
        }

    private var keyOrNullInternal: K? = null
    private var valueOrNullInternal: V? = null

    constructor(statisticsReporter: StatisticsReporter, vararg orderedCursors: CursorInternal<K, V?>) : this(listOf(*orderedCursors), statisticsReporter)

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
        for (cursor in cursorsByPriority) {
            cursor.parent = this
        }

        this.statisticsReporter.reportCursorOpened(CURSOR_NAME)
    }

    override fun invalidatePositionInternal() {
        this.checkIsOpen()
        for (cursor in this.cursorsByPriority) {
            cursor.invalidatePositionInternal()
        }
        this.isValidPosition = false
    }

    override fun firstInternal(): Boolean {
        this.checkIsOpen()

        this.statisticsReporter.reportCursorOperationFirst(CURSOR_NAME)

        // reset all cursors to the start
        this.cursorsByKey = this.createCursorsByKeyMapForDirection(ASCENDING)
        for (cursor in this.cursorsByPriority) {
            if (cursor.firstInternal()) {
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

    override fun lastInternal(): Boolean {
        this.checkIsOpen()

        this.statisticsReporter.reportCursorOperationLast(CURSOR_NAME)

        this.cursorsByKey = this.createCursorsByKeyMapForDirection(DESCENDING)
        for (cursor in this.cursorsByPriority) {
            if (cursor.lastInternal()) {
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
                if (cursor.seekExactlyOrNextInternal(currentKey)) {
                    this.cursorsByKey += cursor
                }
            } else {
                while (cursor.key < currentKey) {
                    if (!cursor.nextInternal()) {
                        cursor.invalidatePositionInternal()
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
                if (cursor.seekExactlyOrPreviousInternal(currentKey)) {
                    this.cursorsByKey += cursor
                }
            } else {
                while (cursor.key > currentKey) {
                    if (!cursor.previousInternal()) {
                        cursor.invalidatePositionInternal()
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
                        if (!cursor.previousInternal()) {
                            // don't add the cursor back into the tree, invalidate it
                            cursor.invalidatePositionInternal()
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
                        if (!cursor.nextInternal()) {
                            // don't add the cursor back into the tree, invalidate it
                            cursor.invalidatePositionInternal()
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

    override fun nextInternal(): Boolean {
        this.checkIsOpen()

        this.statisticsReporter.reportCursorOperationNext(CURSOR_NAME)

        if (!this.isValidPosition) {
            return false
        }

        return this.findValidNextHigherEntry(this.key)
    }

    override fun previousInternal(): Boolean {
        this.checkIsOpen()

        this.statisticsReporter.reportCursorOperationPrevious(CURSOR_NAME)

        if (!this.isValidPosition) {
            return false
        }

        return this.findValidNextLowerEntry(this.key)
    }

    override fun seekExactlyOrNextInternal(key: K): Boolean {
        this.checkIsOpen()

        this.statisticsReporter.reportCursorOperationSeekExactlyOrNext(CURSOR_NAME)

        // reset all cursors to the start
        this.cursorsByKey = this.createCursorsByKeyMapForDirection(ASCENDING)
        for (cursor in this.cursorsByPriority) {
            if (cursor.seekExactlyOrNextInternal(key)) {
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
            this.invalidatePositionInternal()
        }
        return found
    }

    override fun seekExactlyOrPreviousInternal(key: K): Boolean {
        this.checkIsOpen()

        this.statisticsReporter.reportCursorOperationSeekExactlyOrPrevious(CURSOR_NAME)

        // order has changed, re-insert the non-empty ones into the tree
        // (we don't care about the invalid ones, they point to empty keyspaces)
        this.cursorsByKey = this.createCursorsByKeyMapForDirection(DESCENDING)
        for (cursor in this.cursorsByPriority) {
            if (cursor.seekExactlyOrPreviousInternal(key)) {
                this.cursorsByKey += cursor
            }
        }

        if (this.cursorsByKey.isEmpty()) {
            // none of our cursors is valid after calling "seekExactlyOrPrevious()" -> no cursor has any data
            this.invalidatePositionInternal()
            return false
        }

        // remember that we're going in descending direction
        this.currentDirection = DESCENDING

        val found = this.findValidNextLowerEntry()
        if (!found) {
            this.invalidatePositionInternal()
        }
        return found
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

        this.statisticsReporter.reportCursorClosed(CURSOR_NAME)

        this.isOpen = false
        val subCursorCloseHandlers = this.cursorsByPriority.map { it::closeInternal as CloseHandler }
        CursorUtils.executeCloseHandlers(this.closeHandlers + subCursorCloseHandlers)
    }

    private fun createCursorsByKeyMapForDirection(direction: Order): NavigableSet<CursorInternal<K, V?>> {
        return ConcurrentSkipListSet(
            CursorByKeyAndPriorityComparator(
                direction = direction,
                cursorToPriority = cursorToPriority,
            )
        )
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
        private val cursorToPriority: IdentityHashMap<CursorInternal<K, V?>, Int>,
    ) : Comparator<CursorInternal<K, V?>> {

        override fun compare(o1: CursorInternal<K, V?>?, o2: CursorInternal<K, V?>?): Int {
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

        private fun getCursorPriority(cursor: CursorInternal<K, V?>): Int {
            return this.cursorToPriority[cursor]
                ?: error("Cursor is not a child of this overlay cursor: ${cursor}")
        }
    }
}