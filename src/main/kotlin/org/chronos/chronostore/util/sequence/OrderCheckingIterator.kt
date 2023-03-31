package org.chronos.chronostore.util.sequence

class OrderCheckingIterator<T>(
    private val inner: Iterator<T>,
    private val comparator: Comparator<T>,
    private val strict: Boolean
) : Iterator<T> {

    var previous: T? = null

    override fun hasNext(): Boolean {
        return this.inner.hasNext()
    }

    override fun next(): T {
        val next = this.inner.next()
        val prev = this.previous
        if (prev != null) {
            val cmp = this.comparator.compare(prev, next)
            if ((strict && cmp >= 0) || (!strict && cmp > 0)) {
                throw IllegalStateException("The delivered input sequence is not ordered! Received ${prev} before ${next}!")
            }
        }
        this.previous = next
        return next
    }

}