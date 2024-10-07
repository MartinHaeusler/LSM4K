package org.chronos.chronostore.util.comparator

import com.google.common.annotations.VisibleForTesting
import sun.misc.Unsafe
import java.nio.ByteOrder
import java.security.AccessController
import java.security.PrivilegedActionException
import java.security.PrivilegedExceptionAction
import kotlin.math.min

/**
 * This class is inspired by the Guava UnsignedByteComparator.
 *
 * It utilizes the Unsafe (if it is available) to perform SWAR (SIMD Within A Register) operations to compare 8 bytes at once.
 * If the Unsafe is not available, a plain comparator is transparently used instead.
 */
object UnsignedBytesComparator : ByteArrayComparator {

    private val UNSAFE_COMPARATOR_NAME = "${UnsignedBytesComparator::class.java.name}\$UnsafeUnsignedBytesComparator"

    @VisibleForTesting
    val BEST_COMPARATOR = try {
        val theClass = Class.forName(UNSAFE_COMPARATOR_NAME)
        val field = theClass.getField("INSTANCE")
        field.isAccessible = true
        field.get(null) as ByteArrayComparator
    } catch (e: Throwable) {
        BasicUnsignedBytesComparator
    }

    override fun compare(
        left: ByteArray,
        leftFromInclusive: Int,
        leftToInclusive: Int,
        right: ByteArray,
        rightFromInclusive: Int,
        rightToInclusive: Int,
    ): Int {
        return BEST_COMPARATOR.compare(
            left = left,
            leftFromInclusive = leftFromInclusive,
            leftToInclusive = leftToInclusive,
            right = right,
            rightFromInclusive = rightFromInclusive,
            rightToInclusive = rightToInclusive,
        )
    }

    override fun compare(left: ByteArray, right: ByteArray): Int {
        return BEST_COMPARATOR.compare(left, right)
    }


    private data object BasicUnsignedBytesComparator : ByteArrayComparator {

        override fun compare(
            left: ByteArray,
            leftFromInclusive: Int,
            leftToInclusive: Int,
            right: ByteArray,
            rightFromInclusive: Int,
            rightToInclusive: Int,
        ): Int {
            checkArrayFromToPreconditions(left, leftFromInclusive, leftToInclusive, right, rightFromInclusive, rightToInclusive)

            val leftSize = leftToInclusive - leftFromInclusive + 1
            val rightSize = rightToInclusive - rightFromInclusive + 1
            val minLength = min(leftSize, rightSize)

            for (i in 0 until minLength) {
                val leftByte = left[i + leftFromInclusive]
                val rightByte = right[i + rightFromInclusive]
                val result = compareUnsigned(leftByte, rightByte)
                if (result != 0) {
                    return result
                }
            }

            return leftSize - rightSize
        }

        override fun compare(left: ByteArray, right: ByteArray): Int {
            val minLength = min(left.size, right.size)

            for (i in 0 until minLength) {
                val result = compareUnsigned(left[i], right[i])
                if (result != 0) {
                    return result
                }
            }

            return left.size - right.size
        }

    }

    @Suppress("unused") // it's accessed via reflection to allow intercepting any initialization errors
    private data object UnsafeUnsignedBytesComparator : ByteArrayComparator {

        private val BIG_ENDIAN: Boolean = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN
        private val theUnsafe: Unsafe = fetchTheUnsafe()
        private val BYTE_ARRAY_BASE_OFFSET: Int = theUnsafe.arrayBaseOffset(ByteArray::class.java)

        @Suppress("removal")
        private fun fetchTheUnsafe(): Unsafe {
            try {
                return Unsafe.getUnsafe()
            } catch (_: SecurityException) {
                try {
                    return AccessController.doPrivileged(PrivilegedExceptionAction {
                        val unsafeClass = Unsafe::class.java
                        val declaredFields = unsafeClass.declaredFields
                        for (declaredField in declaredFields) {
                            declaredField.isAccessible = true
                            val value = declaredField.get(null) as? Unsafe
                            if (value != null) {
                                return@PrivilegedExceptionAction value
                            }
                        }
                        throw NoSuchFieldError("the Unsafe")
                    }) as Unsafe
                } catch (e: PrivilegedActionException) {
                    throw RuntimeException("Could not initialize intrinsics", e.cause)
                }
            }
        }

        init {
            val is64Bit = System.getProperty("sun.arch.data.model") == "64"
            val isByteOffsetMultipleOf8 = BYTE_ARRAY_BASE_OFFSET % 8 != 0
            val isArrayIndexScaleOne = theUnsafe.arrayIndexScale(ByteArray::class.java) == 1
            if (!is64Bit || isByteOffsetMultipleOf8 || !isArrayIndexScaleOne) {
                error("Cannot use UnsafeUnsignedBytesComparator!")
            }
        }

        override fun compare(
            left: ByteArray,
            leftFromInclusive: Int,
            leftToInclusive: Int,
            right: ByteArray,
            rightFromInclusive: Int,
            rightToInclusive: Int,
        ): Int {
            checkArrayFromToPreconditions(
                left = left,
                leftFromInclusive = leftFromInclusive,
                leftToInclusive = leftToInclusive,
                right = right,
                rightFromInclusive = rightFromInclusive,
                rightToInclusive = rightToInclusive
            )

            val stride = 8
            val leftSize = leftToInclusive - leftFromInclusive + 1
            val rightSize = rightToInclusive - rightFromInclusive + 1
            val minLength = min(leftSize, rightSize)
            val strideLimit = minLength and (stride - 1).inv()
            var i = 0

            val leftStart = BYTE_ARRAY_BASE_OFFSET.toLong() + leftFromInclusive
            val rightStart = BYTE_ARRAY_BASE_OFFSET.toLong() + rightFromInclusive

            while (i < strideLimit) {
                val lw = theUnsafe.getLong(left, leftStart + i.toLong())
                val rw = theUnsafe.getLong(right, rightStart + i.toLong())
                if (lw != rw) {
                    if (BIG_ENDIAN) {
                        return java.lang.Long.compareUnsigned(lw, rw)
                    }

                    val n = java.lang.Long.numberOfTrailingZeros(lw xor rw) and -8
                    return (lw ushr n and 255L).toInt() - (rw ushr n and 255L).toInt()
                }
                i += stride
            }

            while (i < minLength) {
                val result = compareUnsigned(left[i + leftFromInclusive], right[i + rightFromInclusive])
                if (result != 0) {
                    return result
                }

                ++i
            }

            return leftSize - rightSize
        }

        override fun compare(left: ByteArray, right: ByteArray): Int {
            val stride = 8
            val minLength = min(left.size, right.size)
            val strideLimit = minLength and (stride - 1).inv()
            var i = 0
            while (i < strideLimit) {
                val lw = theUnsafe.getLong(left, BYTE_ARRAY_BASE_OFFSET.toLong() + i.toLong())
                val rw = theUnsafe.getLong(right, BYTE_ARRAY_BASE_OFFSET.toLong() + i.toLong())
                if (lw != rw) {
                    if (BIG_ENDIAN) {
                        return java.lang.Long.compareUnsigned(lw, rw)
                    }

                    val n = java.lang.Long.numberOfTrailingZeros(lw xor rw) and -8
                    return (lw ushr n and 255L).toInt() - (rw ushr n and 255L).toInt()
                }
                i += stride
            }

            while (i < minLength) {
                val result = compareUnsigned(left[i], right[i])
                if (result != 0) {
                    return result
                }

                ++i
            }

            return left.size - right.size
        }

    }

    private fun checkArrayFromToPreconditions(
        left: ByteArray,
        leftFromInclusive: Int,
        leftToInclusive: Int,
        right: ByteArray,
        rightFromInclusive: Int,
        rightToInclusive: Int,
    ) {
        when {
            leftFromInclusive < 0 -> error("leftFrom (${leftFromInclusive}) < 0")
            rightFromInclusive < 0 -> error("leftFrom (${leftFromInclusive}) < 0")
            leftFromInclusive > leftToInclusive -> error("leftFrom (${leftFromInclusive}) > leftTo (${leftToInclusive})")
            rightFromInclusive > rightToInclusive -> error("rightFrom (${rightFromInclusive}) > rightTo (${rightToInclusive})")
            leftToInclusive > left.lastIndex -> error("leftTo (${leftToInclusive}) > left.lastIndex (${left.lastIndex})")
            rightToInclusive > right.lastIndex -> error("rightTo (${rightToInclusive}) > right.lastIndex (${right.lastIndex})")
        }
    }

    private fun compareUnsigned(left: Byte, right: Byte): Int {
        val leftValue = (left.toInt() and 0xff)
        val rightValue = (right.toInt() and 0xff)
        return leftValue - rightValue
    }
}