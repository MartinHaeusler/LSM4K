package org.chronos.chronostore.util.cursor

import com.google.common.collect.PeekingIterator
import org.chronos.chronostore.util.Order
import org.chronos.chronostore.util.cursor.CursorUtils.checkIsOpen
import org.chronos.chronostore.util.iterator.IteratorExtensions.peekingIterator
import java.util.*
import kotlin.collections.Map.Entry

class NavigableMapCursor<K : Comparable<K>, V>(
    private val navigableMap: NavigableMap<K, V>,
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

    private var currentIterator: PeekingIterator<Entry<K, V>>? = null
    private var currentIteratorDirection: Order? = null

    private var currentEntry: Entry<K, V>? = null

    override var isOpen: Boolean = true
    private val closeHandlers = mutableListOf<CloseHandler>()

    override val keyOrNull: K?
        get() {
            this.checkIsOpen()
            return this.currentEntry?.key
        }

    override val valueOrNull: V?
        get() {
            this.checkIsOpen()
            return this.currentEntry?.value
        }

    override val isValidPosition: Boolean
        get() = this.currentEntry != null

    override fun invalidatePositionInternal() {
        this.checkIsOpen()
        this.currentEntry = null
    }

    override fun firstInternal(): Boolean {
        this.checkIsOpen()
        val ascendingIterator = this.navigableMap.peekingIterator()
        this.currentIterator = ascendingIterator
        this.currentIteratorDirection = Order.ASCENDING
        if (!ascendingIterator.hasNext()) {
            return false
        }
        this.currentEntry = ascendingIterator.next()
        return true
    }

    override fun lastInternal(): Boolean {
        this.checkIsOpen()
        val descendingIterator = this.navigableMap.descendingMap().peekingIterator()
        this.currentIterator = descendingIterator
        this.currentIteratorDirection = Order.DESCENDING
        if (!descendingIterator.hasNext()) {
            return false
        }
        this.currentEntry = descendingIterator.next()
        return true
    }

    override fun nextInternal(): Boolean {
        this.checkIsOpen()
        return this.moveInternal(Order.ASCENDING)
    }

    override fun previousInternal(): Boolean {
        this.checkIsOpen()
        return this.moveInternal(Order.DESCENDING)
    }

    private fun moveInternal(direction: Order): Boolean {
        val cachedIterator = this.currentIterator
        if (cachedIterator != null && this.currentIteratorDirection == direction) {
            // keep going in the same direction
            if (!cachedIterator.hasNext()) {
                // end of the map has been reached, can't go further in this direction.
                return false
            }
            this.currentEntry = cachedIterator.next()
            return true
        } else {
            // no suitable iterator available, create a new one.
            val start = this.currentEntry
                ?: return false // position is invalid, can't move

            val newIterator = when (direction) {
                Order.ASCENDING -> this.navigableMap.subMap(
                    /* fromKey = */ start.key,
                    /* fromInclusive = */ false,
                    /* toKey = */ this.navigableMap.lastKey(),
                    /* toInclusive = */ true,
                ).peekingIterator()

                Order.DESCENDING -> this.navigableMap.subMap(
                    /* fromKey = */ this.navigableMap.firstKey(),
                    /* fromInclusive = */ true,
                    /* toKey = */ start.key,
                    /* toInclusive = */ false,
                ).descendingMap().peekingIterator()
            }
            if (!newIterator.hasNext()) {
                // no keys in the given direction
                this.currentIterator = null
                this.currentIteratorDirection = null
                return false
            }
            this.currentEntry = newIterator.next()
            this.currentIterator = newIterator
            this.currentIteratorDirection = direction
            return true
        }
    }

    override fun seekExactlyOrPreviousInternal(key: K): Boolean {
        this.checkIsOpen()
        // invalidate the current iterator, if any
        this.currentIterator = null
        this.currentIteratorDirection = null

        val floorEntry = this.navigableMap.floorEntry(key)
            ?: return false // no such entry in the map

        this.currentEntry = floorEntry
        return true
    }

    override fun seekExactlyOrNextInternal(key: K): Boolean {
        this.checkIsOpen()
        // invalidate the current iterator, if any
        this.currentIterator = null
        this.currentIteratorDirection = null


        val ceilEntry = this.navigableMap.ceilingEntry(key)
            ?: return false // no such entry in the map

        this.currentEntry = ceilEntry
        return true
    }

    override fun peekNextInternal(): Pair<K, V>? {
        this.checkIsOpen()
        if (!this.isValidPosition) {
            return null
        }
        val currentIterator = this.currentIterator
        if (currentIterator != null && this.currentIteratorDirection == Order.ASCENDING && currentIterator.hasNext()) {
            // fast path: peek with the iterator
            return currentIterator.peek().toPair()
        }
        // fall back to the default implementation
        if (!this.nextInternal()) {
            return null
        }
        val entry = this.key to this.value
        check(this.previousInternal()) {
            "Illegal Iterator state - move 'previous()' failed!"
        }

        return entry
    }

    override fun peekPreviousInternal(): Pair<K, V>? {
        this.checkIsOpen()
        if (!this.isValidPosition) {
            return null
        }
        val currentIterator = this.currentIterator
        if (currentIterator != null && this.currentIteratorDirection == Order.DESCENDING && currentIterator.hasNext()) {
            // fast path: peek with the iterator
            return currentIterator.peek().toPair()
        }
        // fall back to the default implementation
        if (!this.previousInternal()) {
            return null
        }
        val entry = this.key to this.value
        check(this.nextInternal()) {
            "Illegal Iterator state - move 'next()' failed!"
        }

        return entry
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

}