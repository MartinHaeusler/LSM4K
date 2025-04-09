package org.chronos.chronostore.util.bytes

import com.google.common.collect.Iterators
import com.google.common.hash.HashCode
import com.google.common.hash.HashFunction
import com.google.common.hash.Hasher
import org.chronos.chronostore.impl.annotations.PersistentClass
import org.chronos.chronostore.util.ByteArrayExtensions.hex
import org.chronos.chronostore.util.comparator.UnsignedBytesComparator
import org.chronos.chronostore.util.unit.BinarySize.Companion.Bytes
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.OutputStream
import kotlin.math.min

@PersistentClass(format = PersistentClass.Format.BINARY, details = "Used in many places, including LSM files and WAL files.")
class BasicBytes(
    private val array: ByteArray,
) : Bytes {

    constructor(string: String) : this(string.toByteArray())

    override val size: Int
        get() = this.array.size

    override fun contains(element: Byte): Boolean {
        return this.array.contains(element)
    }

    override operator fun get(index: Int): Byte {
        return this.array[index]
    }

    override fun isEmpty(): Boolean {
        return this.array.isEmpty()
    }

    override fun iterator(): ByteIterator {
        return this.array.iterator()
    }

    override fun lastIndexOf(element: Byte): Int {
        return this.array.lastIndexOf(element)
    }

    override fun indexOf(element: Byte): Int {
        return this.array.indexOf(element)
    }

    override fun writeWithoutSizeTo(outputStream: OutputStream) {
        outputStream.write(this.array)
    }

    override fun writeToOutput(output: BytesOutput) {
        output.write(this.array)
    }

    override fun createInputStream(): InputStream {
        // override for slightly better performance
        return ByteArrayInputStream(this.array)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        return when (other) {
            is BasicBytes -> this.array.contentEquals(other.array)
            is Bytes -> Iterators.elementsEqual(this.iterator(), other.iterator())
            else -> false
        }
    }

    override fun compareTo(other: Bytes): Int {
        if (this.isEmpty()) {
            return if (other.isEmpty()) {
                0
            } else {
                -1
            }
        }
        if (other.isEmpty()) {
            return +1
        }
        return when (other) {
            is SliceBytes -> compareToSliceBytes(other)
            is BasicBytes -> compareToBasicBytes(other)
        }
    }

    private fun compareToSliceBytes(other: SliceBytes): Int {
        return other.compareTo(this) * -1
    }

    private fun compareToBasicBytes(other: BasicBytes): Int {
        return UnsignedBytesComparator.compare(this.array, other.array)
    }

    fun compareTo(array: ByteArray, from: Int, to: Int): Int {
        return UnsignedBytesComparator.compare(
            left = this.array,
            leftFromInclusive = 0,
            leftToInclusive = this.array.lastIndex,
            right = array,
            rightFromInclusive = from,
            rightToInclusive = to,
        )
    }

    override fun hash(hashFunction: HashFunction): HashCode {
        return hashFunction.hashBytes(this.array)
    }

    override fun hash(hasher: Hasher): Hasher {
        return hasher.putBytes(this.array)
    }

    override fun hashCode(): Int {
        return array.contentHashCode()
    }

    override fun toString(): String {
        return if (this.size <= 16) {
            "Bytes[${this.array.hex()}]"
        } else {
            "Bytes[${this.array.hex(16)}... (${this.size.Bytes.toHumanReadableString()})]"
        }
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

        // avoid integer overflow by converting to long
        val end = min((start.toLong() + size.toLong() - 1), this.lastIndex.toLong()).toInt()

        return SliceBytes(this.array, start, end)
    }


    override fun toSharedArrayUnsafe(): ByteArray {
        return this.array
    }

    override fun own(): Bytes {
        // owning makes no difference for basic bytes,
        // because their backing storage is always exactly
        // as big as it needs to be.
        return this
    }

}