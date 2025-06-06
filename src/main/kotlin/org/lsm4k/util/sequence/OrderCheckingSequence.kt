package org.lsm4k.util.sequence

/**
 * Sequence which checks that the [inner] sequence returns elements in their proper order according to the given [comparator].
 */
class OrderCheckingSequence<T>(
    /**
     * The inner sequence to check.
     */
    private val inner: Sequence<T>,
    /**
     * The comparator to use.
     */
    private val comparator: Comparator<T>,
    /**
     * If each element needs to be STRICTLY greater than the previous one, use `true`.
     * If equal elements are allowed in succession, use `false`.
     */
    private val strict: Boolean,
) : Sequence<T> {

    companion object {

        fun <T> Sequence<T>.checkOrdered(strict: Boolean = false): Sequence<T> where T : Comparable<T> {
            return OrderCheckingSequence(this, Comparator.naturalOrder(), strict)
        }

        fun <T> Sequence<T>.checkOrderedDescending(strict: Boolean = false): Sequence<T> where T : Comparable<T> {
            return OrderCheckingSequence(this, Comparator.naturalOrder<T>().reversed(), strict)
        }

        fun <T> Sequence<T>.checkOrdered(comparator: Comparator<T>, strict: Boolean = false): Sequence<T> {
            return OrderCheckingSequence(this, comparator, strict)
        }

        fun <T> Sequence<T>.checkOrderedDescending(comparator: Comparator<T>, strict: Boolean = false): Sequence<T> {
            return OrderCheckingSequence(this, comparator.reversed(), strict)
        }

    }

    override fun iterator(): Iterator<T> {
        return OrderCheckingIterator(this.inner.iterator(), this.comparator, this.strict)
    }

}