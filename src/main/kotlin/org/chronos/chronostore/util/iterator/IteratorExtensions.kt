package org.chronos.chronostore.util.iterator

import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.sequence.DeduplicatingOrderedIterator
import org.chronos.chronostore.util.sequence.LatestVersionOnlyIterator
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

    fun Iterator<Command>.latestVersionOnly(): Iterator<Command> {
        return LatestVersionOnlyIterator(this)
    }

    fun <T> Iterator<T>.filter(predicate: (T) -> Boolean): Iterator<T> {
        return Iterators.filter(this, predicate)
    }

    fun <T> Iterator<T>.toList(): List<T> {
        return this.asSequence().toList()
    }

    fun <T> Iterator<T>.toPeekingIterator(): PeekingIterator<T> {
        return Iterators.peekingIterator(this)
    }

    fun <T> Iterable<T>.peekingIterator(): PeekingIterator<T> {
        return Iterators.peekingIterator(this.iterator())
    }

    fun <K, V> Map<K, V>.peekingIterator(): PeekingIterator<Map.Entry<K, V>> {
        return Iterators.peekingIterator(this.iterator())
    }

}