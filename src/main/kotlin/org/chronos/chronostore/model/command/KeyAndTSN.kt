package org.chronos.chronostore.model.command

import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianIntOrNull
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianLong
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianLong
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.bytes.Bytes.Companion.writeBytesWithoutSize
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

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

    fun writeTo(outputStream: OutputStream){
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

    override fun toString(): String {
        return "${this.key.hex()}@${this.tsn}"
    }

}