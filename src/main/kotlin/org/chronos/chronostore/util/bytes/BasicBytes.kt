package org.chronos.chronostore.util.bytes

import com.google.common.collect.Iterators
import com.google.common.hash.BloomFilter
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.util.ByteArrayExtensions.hex
import org.chronos.chronostore.util.LittleEndianUtil
import org.chronos.chronostore.util.bits.BitTricks.writeStableInt
import org.chronos.chronostore.util.bits.BitTricks.writeStableLong
import org.chronos.chronostore.util.unit.Bytes
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.Charset
import java.util.*
import kotlin.math.min
import kotlin.random.Random

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

    override fun iterator(): Iterator<Byte> {
        return this.array.iterator()
    }

    override fun lastIndexOf(element: Byte): Int {
        return this.array.lastIndexOf(element)
    }

    override fun indexOf(element: Byte): Int {
        return this.array.indexOf(element)
    }

    override fun createInputStream(): InputStream {
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


    override fun toSharedArray(): ByteArray {
        return this.array
    }

    override fun own(): Bytes {
        // owning makes no difference for basic bytes,
        // because their backing storage is always exactly
        // as big as it needs to be.
        return this
    }

}