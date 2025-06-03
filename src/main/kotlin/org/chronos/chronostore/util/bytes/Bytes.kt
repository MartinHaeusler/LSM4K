package org.chronos.chronostore.util.bytes

import com.google.common.hash.BloomFilter
import com.google.common.hash.Hasher
import org.chronos.chronostore.compressor.NoCompressor
import org.chronos.chronostore.impl.annotations.PersistentClass
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.io.vfs.InputSource
import org.chronos.chronostore.util.LittleEndianUtil
import org.chronos.chronostore.util.bits.BitTricks.writeStableInt
import org.chronos.chronostore.util.bits.BitTricks.writeStableLong
import org.chronos.chronostore.util.bytes.Bytes.Companion.wrap
import org.chronos.chronostore.util.hash.Hashable
import org.chronos.chronostore.util.statistics.StatisticsReporter
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.Charset
import java.util.*
import kotlin.math.min
import kotlin.random.Random

/**
 * A [Bytes] object contains raw binary data. It is essentially an immutable version of (or an immutable part of) a [ByteArray].
 *
 * To create a [Bytes] object from a [ByteArray], use [wrap].
 *
 * [Bytes] support arbitrary zero-copy slicing via [slice].
 *
 * If a [Bytes] object should be kept for extended periods of time, please use [own] to
 * avoid dependencies on large buffers which would prevent their garbage collection.
 */
@PersistentClass(format = PersistentClass.Format.BINARY, details = "Used in many places, including LSM files and WAL files.")
sealed interface Bytes :
    Comparable<Bytes>,
    Collection<Byte>,
    InputSource,
    Hashable {

    companion object {

        private val SIZE_SUFFIXES = listOf("KiB", "MiB", "GiB", "TiB", "PiB", "XiB")

        val EMPTY: Bytes = BasicBytes(ByteArray(0))

        val TRUE: Bytes = BasicBytes(byteArrayOf(1))

        val FALSE: Bytes = BasicBytes(byteArrayOf(0))

        fun of(vararg bytes: Byte): Bytes {
            return BasicBytes(byteArrayOf(*bytes))
        }

        fun of(text: String): Bytes {
            return BasicBytes(text.toByteArray())
        }

        /**
         * Creates a new [Bytes] object that wraps the given [ByteArray].
         *
         * **ATTENTION:** By calling this method, ownership of the byte array is given
         * entirely to the new [Bytes] object. For performance reasons, there is no
         * defensive copy being created here.
         *
         * **DO NOT MODIFY THE ARRAY AFTER PASSING IT TO THIS METHOD!**.
         *
         * @param byteArray The byte array to wrap into a new [Bytes] object. **Do not modify it after passing it to this method!**
         *
         * @return The newly created [Bytes] object.
         */
        fun wrap(byteArray: ByteArray): Bytes {
            return BasicBytes(byteArray)
        }

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
            return wrap(bytes)
        }

        @Suppress("UnstableApiUsage")
        fun BloomFilter<ByteArray>.mightContain(bytes: Bytes): Boolean {
            return this.mightContain(bytes.toSharedArrayUnsafe())
        }

        @Suppress("UnstableApiUsage")
        fun BloomFilter<ByteArray>.put(bytes: Bytes) {
            this.put(bytes.toSharedArrayUnsafe())
        }

        fun OutputStream.writeBytesWithoutSize(bytes: Bytes) {
            bytes.writeWithoutSizeTo(this)
        }

        fun Bytes.compressWith(algorithm: CompressionAlgorithm, statisticsReporter: StatisticsReporter): Bytes {
            require(!this.isEmpty()) { "The empty Bytes object cannot be compressed!" }
            if (algorithm.compressor is NoCompressor) {
                return this
            }
            return BasicBytes(algorithm.compress(this.toSharedArrayUnsafe(), statisticsReporter))
        }

        fun Bytes.decompressWith(algorithm: CompressionAlgorithm, statisticsReporter: StatisticsReporter): Bytes {
            require(!this.isEmpty()) { "The empty Bytes object cannot be decompressed!" }
            if (algorithm.compressor is NoCompressor) {
                return this
            }
            return wrap(algorithm.decompress(toSharedArrayUnsafe(), statisticsReporter))
        }


        fun Bytes.decompressWith(
            algorithm: CompressionAlgorithm,
            uncompressedSize: Int,
            statisticsReporter: StatisticsReporter,
        ): Bytes {
            require(!this.isEmpty()) { "The empty Bytes object cannot be decompressed!" }
            require(uncompressedSize > 0) { "The uncompressedSize (${uncompressedSize}) must be greater than zero!" }
            if (algorithm.compressor is NoCompressor) {
                return this
            }
            val output = TrackingBytesOutput()
            this.writeToOutput(output)

            val compressedBytes = output.lastBytes
                ?: throw IllegalStateException("Bytes.writeToOutput produced no data!")

            val compressedBytesOffset = output.lastOffset ?: 0
            val compressedBytesLength = output.lastLength ?: compressedBytes.size

            val target = ByteArray(uncompressedSize)

            algorithm.decompress(compressedBytes, compressedBytesOffset, compressedBytesLength, target, statisticsReporter)

            return wrap(target)
        }

        fun stableInt(int: Int): Bytes {
            val byteArray = ByteArrayOutputStream().use { baos ->
                baos.writeStableInt(int)
                baos.toByteArray()
            }
            return wrap(byteArray)
        }

        fun stableLong(long: Long): Bytes {
            val byteArray = ByteArrayOutputStream().use { baos ->
                baos.writeStableLong(long)
                baos.toByteArray()
            }
            return wrap(byteArray)
        }

        /**
         * Puts the given [bytes] object into this hasher.
         *
         * @param bytes The bytes to add to the hash
         *
         * @return The hasher
         */
        fun Hasher.putBytes(bytes: Bytes): Hasher {
            bytes.hash(this)
            return this
        }

    }


    /** Returns the number of bytes in this object. */
    override val size: Int

    /**
     *  Gets the [Byte] at the given [index].
     *
     *  @param index The index of the Byte to get. Must not be negative, must be less than [size].
     *
     *  @return The byte at the index.
     *
     *  @throws IndexOutOfBoundsException if the [index] is not within the expected bounds.
     */
    operator fun get(index: Int): Byte

    /**
     * Returns the last valid index of this object, or -1 if this object is empty.
     */
    val lastIndex: Int
        get() {
            val size = this.size
            return if (size <= 0) {
                -1
            } else {
                size - 1
            }
        }

    fun readLittleEndianInt(position: Int): Int {
        return LittleEndianUtil.readLittleEndianInt(
            this[position + 0],
            this[position + 1],
            this[position + 2],
            this[position + 3],
        )
    }

    fun readLittleEndianLong(position: Int): Long {
        return LittleEndianUtil.readLittleEndianLong(
            this[position + 0],
            this[position + 1],
            this[position + 2],
            this[position + 3],
            this[position + 4],
            this[position + 5],
            this[position + 6],
            this[position + 7],
        )
    }


    /**
     *  Converts this Bytes object into a raw byte array.
     *
     *  Please note that the array may be **shared** with other processes.
     *
     *  **DO NOT MODIFY THE RETURNED ARRAY!**
     *
     *  @return A [ByteArray] which may be the internal representation of this object.
     */
    fun toSharedArrayUnsafe(): ByteArray

    /**
     * Converts this [Bytes] object into a raw byte array.
     *
     * The returned array is guaranteed to be a "fresh" copy which is not
     * shared with anybody else. Each invocation of this method will return
     * a new copy of the array.
     *
     * @return A fresh copy of a [ByteArray] which contains all bytes in this
     *         object and  can be modified in any way without affecting the
     *         inner state of this object.
     */
    fun toOwnedArray(): ByteArray {
        val ownedArray = ByteArray(this.size)
        System.arraycopy(this.toSharedArrayUnsafe(), 0, ownedArray, 0, this.size)
        return ownedArray
    }

    /**
     * Creates a slice of this object, i.e. a view which is limited to the given boundaries.
     *
     * @param start The zero-based start index where the slice should start.
     * @param size The size of the slice. Must not be negative. If the desired size exceeds the length of this [Bytes] object,
     *             the resulting slice will contain all remaining bytes of this object.
     *
     * @return The slice, a view on this object within the given boundaries.
     */
    fun slice(start: Int, size: Int = Int.MAX_VALUE): Bytes

    /**
     * Creates a slice of this object, i.e. a view which is limited to the given boundaries.
     *
     * @param range The range of indices to return.
     *
     * @return The slice, a view on this object within the given boundaries.
     */
    fun slice(range: IntRange): Bytes {
        return slice(range.first, range.last - range.first + 1)
    }

    /**
     * Returns the hexadecimal representation of this object as a String.
     *
     * @return The hexadecimal representation of this object as a String.
     */
    fun hex(): String {
        return HexFormat.of().formatHex(this.toSharedArrayUnsafe())
    }

    /**
     * Converts the content of this [Bytes] object to a string with the given [charset].
     *
     * Please note that this operation may fail if the bytes do not represent a valid
     * string under the given charset.
     *
     * @param charset The charset to use.
     *
     * @return The String representation of this object.
     */
    fun asString(charset: Charset = Charsets.UTF_8): String {
        // the constructor for String internally creates its own
        // copy of the array we pass in, so it's okay to use the
        // shared array version here.
        return String(this.toSharedArrayUnsafe(), charset)
    }

    fun lastIndexOf(element: Byte): Int {
        return this.iterator().asSequence().lastIndexOf(element)
    }

    fun indexOf(element: Byte): Int {
        return this.iterator().asSequence().indexOf(element)
    }

    override fun containsAll(elements: Collection<Byte>): Boolean {
        for (element in elements) {
            if (element !in this) {
                return false
            }
        }
        return true
    }

    override fun isEmpty(): Boolean {
        return !this.iterator().hasNext()
    }

    override fun iterator(): ByteIterator

    /**
     * Writes the contents of this [Bytes] object to the given output stream.
     *
     * This writes occurs in verbatim, i.e. there is no prefixing with a size
     * or any other modification. It's a direct copy process.
     *
     * If this object [isEmpty], no bytes will be written at all.
     */
    fun writeWithoutSizeTo(outputStream: OutputStream)

    fun writeToOutput(output: BytesOutput)


    /**
     * Creates an [InputStream] which delivers all [Byte]s in this object.
     *
     * @retun The input stream.
     */
    override fun createInputStream(): InputStream {
        return BytesInputStream(this.iterator())
    }

    override fun compareTo(other: Bytes): Int {
        // the following implementation has been borrowed from Xodus
        // and slightly adapted, but RocksDB uses the same comparison technique.
        val min = min(this.size, other.size)

        for (i in 0..<min) {
            val myByteUnsigned = this[i].toInt() and 0xff
            val otherByteUnsigned = other[i].toInt() and 0xff
            val cmp = myByteUnsigned - otherByteUnsigned
            if (cmp != 0) {
                return cmp
            }
        }

        return this.size - other.size
    }

    /**
     * Creates an "owned" copy of this [Bytes] object.
     *
     * This guarantees that the backing storage will contain exactly the required bytes,
     * and no other bytes. This is mostly relevant for [SliceBytes].
     */
    fun own(): Bytes

}