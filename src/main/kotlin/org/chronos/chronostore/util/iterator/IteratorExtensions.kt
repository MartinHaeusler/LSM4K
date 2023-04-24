package org.chronos.chronostore.util.iterator

import org.chronos.chronostore.util.sequence.DeduplicatingOrderedIterator
import org.chronos.chronostore.util.sequence.OrderCheckingIterator

object IteratorExtensions {

    fun <T : Comparable<T>> Iterator<T>.checkOrdered(strict: Boolean = true): Iterator<T> {
        return OrderCheckingIterator(this, Comparator.naturalOrder(), strict)
    }

    fun <T> Iterator<T>.checkOrdered(comparator: Comparator<T>, strict: Boolean = true): Iterator<T> {
        return OrderCheckingIterator(this, comparator, strict)
    }

    fun <T> Iterator<T>.orderedDistinct(): Iterator<T> {
        return DeduplicatingOrderedIterator(this)
    }

}