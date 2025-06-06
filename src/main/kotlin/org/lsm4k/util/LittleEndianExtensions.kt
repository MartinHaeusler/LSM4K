package org.lsm4k.util

import org.lsm4k.api.exceptions.TruncatedInputException
import org.lsm4k.util.IOExtensions.readByte
import org.lsm4k.util.bytes.Bytes
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream

object LittleEndianExtensions {

    fun littleEndianIntAsBytes(int: Int): Bytes {
        return ByteArrayOutputStream().use { baos ->
            baos.writeLittleEndianInt(int)
            Bytes.wrap(baos.toByteArray())
        }
    }

    @JvmStatic
    fun OutputStream.writeLittleEndianInt(int: Int): OutputStream {
        // The strategy here is as follows:
        // - shift the bytes of the long such that the relevant byte is at the LAST (rightmost position)
        // - then apply a bitmask that eliminates all bits except for the last 8
        // - write that into the output, then continue with the next byte in the same fashion.
        this.write((int.ushr(0 * 8) and 0xFF))
        this.write((int.ushr(1 * 8) and 0xFF))
        this.write((int.ushr(2 * 8) and 0xFF))
        this.write((int.ushr(3 * 8) and 0xFF))
        return this
    }

    @JvmStatic
    fun OutputStream.writeLittleEndianLong(long: Long): OutputStream {
        // The strategy here is as follows:
        // - shift the bytes of the long such that the relevant byte is at the LAST (rightmost position)
        // - then apply a bitmask that eliminates all bits except for the last 8
        // - write that into the output, then continue with the next byte in the same fashion.
        this.write((long.ushr(0 * 8) and 0xFF).toInt())
        this.write((long.ushr(1 * 8) and 0xFF).toInt())
        this.write((long.ushr(2 * 8) and 0xFF).toInt())
        this.write((long.ushr(3 * 8) and 0xFF).toInt())
        this.write((long.ushr(4 * 8) and 0xFF).toInt())
        this.write((long.ushr(5 * 8) and 0xFF).toInt())
        this.write((long.ushr(6 * 8) and 0xFF).toInt())
        this.write((long.ushr(7 * 8) and 0xFF).toInt())
        return this
    }

    @JvmStatic
    fun OutputStream.writeLittleEndianDouble(double: Double): OutputStream {
        return this.writeLittleEndianLong(double.toBits())
    }

    @JvmStatic
    fun InputStream.readLittleEndianLong(): Long {
        return readLittleEndianLongOrNull()
            ?: throw TruncatedInputException("End of input has been reached!")
    }

    @JvmStatic
    fun InputStream.readLittleEndianLongOrNull(): Long? {
        return LittleEndianUtil.readLittleEndianLong(
            this.readByte() ?: return null,
            this.readByte() ?: return null,
            this.readByte() ?: return null,
            this.readByte() ?: return null,
            this.readByte() ?: return null,
            this.readByte() ?: return null,
            this.readByte() ?: return null,
            this.readByte() ?: return null,
        )
    }

    @JvmStatic
    fun InputStream.readLittleEndianDouble(): Double {
        return this.readLittleEndianDoubleOrNull()
            ?: throw TruncatedInputException("End of input has been reached!")
    }

    @JvmStatic
    fun InputStream.readLittleEndianDoubleOrNull(): Double? {
        return LittleEndianUtil.readLittleEndianDouble(
            this.readByte() ?: return null,
            this.readByte() ?: return null,
            this.readByte() ?: return null,
            this.readByte() ?: return null,
            this.readByte() ?: return null,
            this.readByte() ?: return null,
            this.readByte() ?: return null,
            this.readByte() ?: return null,
        )
    }

    @JvmStatic
    fun InputStream.readLittleEndianInt(): Int {
        return this.readLittleEndianIntOrNull()
            ?: throw TruncatedInputException("End of input has been reached!")
    }

    @JvmStatic
    fun InputStream.readLittleEndianIntOrNull(): Int? {
        return LittleEndianUtil.readLittleEndianInt(
            this.readByte() ?: return null,
            this.readByte() ?: return null,
            this.readByte() ?: return null,
            this.readByte() ?: return null,
        )
    }

}