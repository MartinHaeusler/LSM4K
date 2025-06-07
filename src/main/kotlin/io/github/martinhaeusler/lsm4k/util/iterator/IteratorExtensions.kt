package io.github.martinhaeusler.lsm4k.util.iterator

import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.util.sequence.DeduplicatingOrderedIterator
import io.github.martinhaeusler.lsm4k.util.sequence.DropHistoryOlderThanIterator
import io.github.martinhaeusler.lsm4k.util.sequence.LatestVersionOnlyIterator
import io.github.martinhaeusler.lsm4k.util.sequence.OrderCheckingIterator

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

    fun Iterator<Command>.dropHistoryOlderThan(tsn: TSN): Iterator<Command> {
        return DropHistoryOlderThanIterator(this, tsn)
    }

    fun <T> Iterator<T>.filter(predicate: (T) -> Boolean): Iterator<T> {
        return Iterators.filter(this, predicate)
    }

    fun <T> Iterator<T>.toList(): List<T> {
        return this.asSequence().toList()
    }

    fun <T> Iterator<T>.toMutableList(): MutableList<T> {
        return this.asSequence().toMutableList()
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

    fun <T> PeekingIterator<T>.peekOrNull(): T? {
        return if (this.hasNext()) {
            this.peek()
        } else {
            null
        }
    }

    fun <T, R> Iterator<T>.map(map: (T) -> R): Iterator<R> {
        return Iterators.transform(this, map)
    }

    fun <T> Iterator<T>.onEach(action: (T) -> Unit): Iterator<T> {
        return this.map {
            // apply the action...
            action(it)
            // ... but keep the element the same
            it
        }
    }

}