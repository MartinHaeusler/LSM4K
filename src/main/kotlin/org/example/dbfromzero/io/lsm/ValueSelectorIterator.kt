package org.example.dbfromzero.io.lsm

import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import org.chronos.chronostore.util.Bytes

class ValueSelectorIterator : Iterator<Pair<Bytes, Bytes>> {

    companion object {

        fun createSortedAndSelectedIterator(orderedReaders: List<KeyValueFileReader>): ValueSelectorIterator {
            val rankedIterators = orderedReaders.asSequence()
                .mapIndexed { index, reader -> index to KeyValueFileIterator(reader) }
                .map { addRank(it.second, it.first) }
                .toList()
            val mergeSortedIterator = Iterators.mergeSorted(rankedIterators, compareBy { it.key })
            return ValueSelectorIterator(mergeSortedIterator)
        }

        private fun addRank(iterator: Iterator<Pair<Bytes, Bytes>>, rank: Int): Iterator<KeyValueRank> {
            return Iterators.transform(iterator) { KeyValueRank(it.first, it.second, rank) }
        }

    }

    private val sortedIterator: PeekingIterator<KeyValueRank>

    private constructor(sortedIterator: Iterator<KeyValueRank>) {
        this.sortedIterator = Iterators.peekingIterator(sortedIterator)
    }

    override fun hasNext(): Boolean {
        return this.sortedIterator.hasNext()
    }

    override fun next(): Pair<Bytes, Bytes> {
        val first = sortedIterator.next()
        val key = first.key
        var highestRank = first.rank
        var highestRankValue = first.value
        while (sortedIterator.hasNext() && sortedIterator.peek().key == key) {
            val entry = sortedIterator.next()
            if (entry.rank > highestRank) {
                highestRank = entry.rank
                highestRankValue = entry.value
            }
        }
        return Pair(key, highestRankValue)
    }


    private class KeyValueRank(
        val key: Bytes,
        val value: Bytes,
        val rank: Int
    )

}