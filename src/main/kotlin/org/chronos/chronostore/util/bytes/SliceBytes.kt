package org.chronos.chronostore.util.bytes

import com.google.common.collect.Iterators
import org.chronos.chronostore.util.ByteArrayExtensions.hex
import org.chronos.chronostore.util.unit.Bytes
import java.io.OutputStream
import kotlin.math.min

class SliceBytes(
    private val array: ByteArray,
    private val startInclusive: Int,
    private val endInclusive: Int
) : Bytes {

    override val size: Int = this.endInclusive - this.startInclusive + 1

    init {
        require(startInclusive in 0..array.lastIndex) { "Argument 'startInclusive' (${startInclusive}) must be in range [0..${array.lastIndex}]!" }
        require(endInclusive in startInclusive..array.lastIndex) { "Argument 'endInclusive' ${endInclusive} must be in range [${startInclusive}..${array.lastIndex}]!" }
    }

    private val sharedArray by lazy(LazyThreadSafetyMode.NONE) {
        this.array.sliceArray(startInclusive..endInclusive)
    }

    override fun get(index: Int): Byte {
        require(index in 0 until this.size) { "Argument '${index}' must be within [0..${this.lastIndex}]!" }
        val actualIndex = index + this.startInclusive
        return this.array[actualIndex]
    }

    override fun toSharedArray(): ByteArray {
        return sharedArray
    }

    override fun own(): Bytes {
        return BasicBytes(this.toSharedArray())
    }

    override fun writeWithoutSizeTo(outputStream: OutputStream) {
        outputStream.write(this.array, this.startInclusive, this.size)
    }

    override fun slice(start: Int, size: Int): Bytes {
        require(start >= 0) { "Argument 'start' (${start}) must not be negative!" }
        require(start in 0..this.lastIndex) { "Argument 'start' (${start}) is outside the expected range [0..${this.lastIndex}]!" }
        require(size >= 0) { "Argument 'size' (${size}) must not be negative!" }

        if (size == 0) {
            return Bytes.EMPTY
        }
        if (start == 0 && size >= this.size) {
            // "whole" slice
            return this
        }

        // Note that we're not doing "slice of slice" here.
        // Instead, we're creating a (smaller) slice of the original byte array.
        // The new object will be completely unrelated to "this", except that
        // it shares the same underlying array.
        val actualStart = this.startInclusive + start
        // avoid integer overflow by converting to long
        val end = min(actualStart.toLong() + size.toLong()-1, this.endInclusive.toLong()).toInt()


        return SliceBytes(this.array, actualStart, end)
    }

    override fun contains(element: Byte): Boolean {
        for (i in startInclusive..endInclusive) {
            if (array[i] == element) {
                return true
            }
        }
        return false
    }

    override fun hashCode(): Int {
        // same as Arrays.contentHashCode(...).
        var result = 1
        for (element in this) result = 31 * result + element
        return result
    }

    override fun equals(other: Any?): Boolean {
        if (other == null) {
            return false
        }
        if (other === this) {
            return true
        }

        if (other !is Bytes) {
            return false
        }

        return Iterators.elementsEqual(this.iterator(), other.iterator())
    }

    override fun toString(): String {
        return if (this.size <= 16) {
            "Bytes[${this.sharedArray.hex()})]"
        } else {
            "Bytes[${this.sharedArray.hex(16)}... (${this.size.Bytes.toHumanReadableString()})]"
        }
    }

    override fun iterator(): ByteIterator {
        return SliceBytesIterator()
    }

    private inner class SliceBytesIterator : ByteIterator() {

        var current = this@SliceBytes.startInclusive

        override fun hasNext(): Boolean {
            return current <= endInclusive
        }

        override fun nextByte(): Byte {
            if (current > endInclusive) {
                throw NoSuchElementException("Iterator has no more elements!")
            }
            val element = array[current]
            current++
            return element
        }

    }

}