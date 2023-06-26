package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.util.Order
import java.util.*
import kotlin.collections.Map.Entry

class NavigableMapCursor<K : Comparable<K>, V>(
    private val navigableMap: NavigableMap<K, V>,
) : AbstractCursor<K, V>() {

    private var currentIterator: Iterator<Entry<K, V>>? = null
    private var currentIteratorDirection: Order? = null

    private var currentEntry: Entry<K, V>? = null


    override val keyOrNullInternal: K?
        get() = this.currentEntry?.key

    override val valueOrNullInternal: V?
        get() = this.currentEntry?.value

    override fun closeInternal() {
        // nothing to do, everything is in-memory.
    }

    override fun firstInternal(): Boolean {
        val ascendingIterator = this.navigableMap.iterator()
        this.currentIterator = ascendingIterator
        this.currentIteratorDirection = Order.ASCENDING
        if (!ascendingIterator.hasNext()) {
            return false
        }
        this.currentEntry = ascendingIterator.next()
        return true
    }

    override fun lastInternal(): Boolean {
        val descendingIterator = this.navigableMap.descendingMap().iterator()
        this.currentIterator = descendingIterator
        this.currentIteratorDirection = Order.DESCENDING
        if (!descendingIterator.hasNext()) {
            return false
        }
        this.currentEntry = descendingIterator.next()
        return true
    }

    override fun moveInternal(direction: Order): Boolean {
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
                Order.ASCENDING -> this.navigableMap.subMap(start.key, false, this.navigableMap.lastKey(), true).iterator()
                Order.DESCENDING -> this.navigableMap.subMap(this.navigableMap.firstKey(), true, start.key, false).descendingMap().iterator()
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
        // invalidate the current iterator, if any
        this.currentIterator = null
        this.currentIteratorDirection = null

        val floorEntry = this.navigableMap.floorEntry(key)
            ?: return false // no such entry in the map

        this.currentEntry = floorEntry
        return true
    }

    override fun seekExactlyOrNextInternal(key: K): Boolean {
        // invalidate the current iterator, if any
        this.currentIterator = null
        this.currentIteratorDirection = null


        val ceilEntry = this.navigableMap.ceilingEntry(key)
            ?: return false // no such entry in the map

        this.currentEntry = ceilEntry
        return true
    }


}