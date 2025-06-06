package org.lsm4k.util.collection

class RingBuffer<T : Any>(
    size: Int,
) : Iterable<T> {

    init {
        require(size > 0) {
            "Argument 'size' (${size}) must be positive!"
        }
    }

    private val array = arrayOfNulls<Any>(size)

    private var position: Int = 0

    var isFull: Boolean = false
        private set

    val size: Int
        get() = this.array.size

    val elementCount: Int
        get() = this.array.count { it != null }

    fun clear() {
        for (i in this.array.indices) {
            this.array[i] = null
        }
        this.position = 0
        this.isFull = false
    }

    @Suppress("UNCHECKED_CAST")
    fun add(element: T): T? {
        val oldElement = this.array[position]
        this.array[position] = element
        if (position == array.lastIndex) {
            // we filled the array!
            this.isFull = true
        }
        position = (position + 1) % this.size
        return oldElement as T?
    }

    @Suppress("UNCHECKED_CAST")
    override fun iterator(): Iterator<T> {
        return array.asSequence().filterNotNull().iterator() as Iterator<T>
    }

}