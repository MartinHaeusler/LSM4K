package org.lsm4k.model.command

import org.lsm4k.impl.annotations.PersistentClass
import org.lsm4k.util.IOExtensions.withInputStream
import org.lsm4k.util.LittleEndianExtensions.readLittleEndianInt
import org.lsm4k.util.LittleEndianExtensions.readLittleEndianIntOrNull
import org.lsm4k.util.LittleEndianExtensions.readLittleEndianLong
import org.lsm4k.util.LittleEndianExtensions.writeLittleEndianInt
import org.lsm4k.util.LittleEndianExtensions.writeLittleEndianLong
import org.lsm4k.util.TSN
import org.lsm4k.util.bytes.Bytes
import org.lsm4k.util.bytes.Bytes.Companion.writeBytesWithoutSize
import org.lsm4k.util.report.HexKeyAndTSN
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

@PersistentClass(format = PersistentClass.Format.BINARY, "Used in LSM file headers.")
data class KeyAndTSN(
    val key: Bytes,
    val tsn: TSN,
) : Comparable<KeyAndTSN> {

    companion object {

        fun readFromBytes(bytes: Bytes): KeyAndTSN {
            bytes.withInputStream { inputStream ->
                return readFromStream(inputStream)
            }
        }

        fun readFromBytesOrNull(bytes: Bytes): KeyAndTSN? {
            bytes.withInputStream { inputStream ->
                return readFromStreamOrNull(inputStream)
            }
        }

        fun readFromStreamOrNull(inputStream: InputStream): KeyAndTSN? {
            val keyLength = inputStream.readLittleEndianIntOrNull()
                ?: return null
            val keyArray = ByteArray(keyLength)
            val readBytes = inputStream.read(keyArray)
            if (readBytes != keyLength) {
                return null
            }
            val tsn = inputStream.readLittleEndianLong()
            return KeyAndTSN(Bytes.wrap(keyArray), tsn)
        }

        fun readFromStream(inputStream: InputStream): KeyAndTSN {
            val keyLength = inputStream.readLittleEndianInt()
            val keyArray = ByteArray(keyLength)
            val readBytes = inputStream.read(keyArray)
            if (readBytes != keyLength) {
                throw IOException("Failed to read ${keyLength} bytes from input stream (got: ${readBytes})!")
            }
            val tsn = inputStream.readLittleEndianLong()
            return KeyAndTSN(Bytes.wrap(keyArray), tsn)
        }

    }

    override fun compareTo(other: KeyAndTSN): Int {
        val keyCmp = this.key.compareTo(other.key)
        if (keyCmp != 0) {
            return keyCmp
        }
        return this.tsn.compareTo(other.tsn)
    }

    fun toBytes(): Bytes {
        val baos = ByteArrayOutputStream(this.byteSize)
        this.writeTo(baos)
        return Bytes.wrap(baos.toByteArray())
    }

    fun writeTo(outputStream: OutputStream) {
        outputStream.writeLittleEndianInt(this.key.size)
        outputStream.writeBytesWithoutSize(this.key)
        outputStream.writeLittleEndianLong(this.tsn)
    }

    val byteSize: Int
        get() {
            return 4 + // int prefix, indicating key length.
                key.size + // bytes of the key
                8 // tsn bytes
        }

    fun hex(): HexKeyAndTSN {
        return HexKeyAndTSN(this.key.hex(), this.tsn)
    }

    override fun toString(): String {
        return "${this.key.hex()}@${this.tsn}"
    }

}