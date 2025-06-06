package org.lsm4k.util.sequence

class DeduplicatingOrderedIterator<T>(
    private val inner: Iterator<T>
) : Iterator<T> {

    private var next: T? = null

    init {
        this.moveNext()
    }

    private fun moveNext() {
        while (this.inner.hasNext()) {
            val innerNext = this.inner.next()
            if (this.next != innerNext) {
                this.next = innerNext
                return // found
            }
        }
        // not found
        this.next = null
    }

    override fun hasNext(): Boolean {
        return this.next != null
    }

    override fun next(): T {
        if (!this.hasNext()) {
            throw NoSuchElementException("Iterator is exhausted")
        }
        val result = this.next!!
        this.moveNext()
        return result
    }

}