package org.example.dbfromzero.util

import org.example.dbfromzero.io.vfs.InputSource
import org.example.dbfromzero.util.ByteArrayExtensions.hex
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.*
import kotlin.math.min

class Bytes(
    private val array: ByteArray
) : Comparable<Bytes>, Collection<Byte>, InputSource {

    companion object {

        private val SIZE_SUFFIXES = listOf("KiB", "MiB", "GiB", "TiB", "PiB", "XiB")

        val EMPTY: Bytes = Bytes(ByteArray(0))

        fun formatSize(size: Long): String {
            var scaled = size.toDouble()
            val iter = SIZE_SUFFIXES.iterator()
            var suffix: String? = ""
            while (scaled >= 100.0 && iter.hasNext()) {
                scaled /= 1024.0
                suffix = iter.next()
            }
            return String.format("%.2f%s", scaled, suffix)
        }

        fun random(random: Random, length: Int): Bytes {
            val bytes = ByteArray(length)
            random.nextBytes(bytes)
            return Bytes(bytes)
        }

    }

    override val size: Int
        get() = this.array.size

    override fun containsAll(elements: Collection<Byte>): Boolean {
        for (element in elements) {
            if (!this.array.contains(element)) {
                return false
            }
        }
        return true
    }

    override fun contains(element: Byte): Boolean {
        return this.array.contains(element)
    }

    operator fun get(index: Int): Byte {
        return this.array[index]
    }

    override fun compareTo(other: Bytes): Int {
        // the following implementation has been borrowed from Xodus
        // and slightly adapted, but RocksDB uses the same comparison technique.
        val min = min(this.size, other.size)

        for (i in 0 until min) {
            val myByteUnsigned = this[i].toInt() and 0xff
            val otherByteUnsigned = other[i].toInt() and 0xff
            val cmp = myByteUnsigned - otherByteUnsigned
            if (cmp != 0) {
                return cmp
            }
        }

        return this.size - other.size
    }

    override fun isEmpty(): Boolean {
        return this.array.isEmpty()
    }

    override fun iterator(): Iterator<Byte> {
        return this.array.iterator()
    }

    fun lastIndexOf(element: Byte): Int {
        return this.array.lastIndexOf(element)
    }

    fun indexOf(element: Byte): Int {
        return this.array.indexOf(element)
    }

    override fun createInputStream(): InputStream {
        return ByteArrayInputStream(this.array)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Bytes

        if (!array.contentEquals(other.array)) return false

        return true
    }

    override fun hashCode(): Int {
        return array.contentHashCode()
    }

    fun hex(): String {
        return HexFormat.of().formatHex(this.array)
    }

    override fun toString(): String {
        return "Bytes[${this.array.hex()}]"
    }

    fun writeToStream(outputStream: OutputStream) {
        // this is always safe, because output streams
        // treat the incoming arrays as read-only.
        outputStream.write(this.array)
    }

    fun readLittleEndianLong(position: Int = 0): Long {
        return LittleEndianUtil.readLittleEndianLong(
            this.array[position + 0],
            this.array[position + 1],
            this.array[position + 2],
            this.array[position + 3],
            this.array[position + 4],
            this.array[position + 5],
            this.array[position + 6],
            this.array[position + 7],
        )
    }

}