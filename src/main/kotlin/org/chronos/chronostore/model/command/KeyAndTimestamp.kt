package org.chronos.chronostore.model.command

import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.Bytes.Companion.write
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianIntOrNull
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianLong
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianLong
import org.chronos.chronostore.util.Timestamp
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

data class KeyAndTimestamp(
    val key: Bytes,
    val timestamp: Timestamp,
) : Comparable<KeyAndTimestamp> {

    companion object {

        fun readFromBytes(bytes: Bytes): KeyAndTimestamp {
            bytes.withInputStream { inputStream ->
                return readFromStream(inputStream)
            }
        }

        fun readFromStreamOrNull(inputStream: InputStream): KeyAndTimestamp? {
            val keyLength = inputStream.readLittleEndianIntOrNull()
                ?: return null
            val keyArray = ByteArray(keyLength)
            val readBytes = inputStream.read(keyArray)
            if (readBytes != keyLength) {
                throw IOException("Failed to read ${keyLength} bytes from input stream (got: ${readBytes})!")
            }
            val timestamp = inputStream.readLittleEndianLong()
            return KeyAndTimestamp(Bytes(keyArray), timestamp)
        }

        fun readFromStream(inputStream: InputStream): KeyAndTimestamp {
            val keyLength = inputStream.readLittleEndianInt()
            val keyArray = ByteArray(keyLength)
            val readBytes = inputStream.read(keyArray)
            if (readBytes != keyLength) {
                throw IOException("Failed to read ${keyLength} bytes from input stream (got: ${readBytes})!")
            }
            val timestamp = inputStream.readLittleEndianLong()
            return KeyAndTimestamp(Bytes(keyArray), timestamp)
        }

    }

    override fun compareTo(other: KeyAndTimestamp): Int {
        val keyCmp = this.key.compareTo(other.key)
        if (keyCmp != 0) {
            return keyCmp
        }
        return this.timestamp.compareTo(other.timestamp)
    }

    fun toBytes(): Bytes {
        val baos = ByteArrayOutputStream(this.byteSize)
        this.writeTo(baos)
        return Bytes(baos.toByteArray())
    }

    fun writeTo(outputStream: OutputStream){
        outputStream.writeLittleEndianInt(this.key.size)
        outputStream.write(this.key)
        outputStream.writeLittleEndianLong(this.timestamp)
    }

    private val byteSize: Int
        get() {
            return 4 + // int prefix, indicating key length.
                key.size + // bytes of the key
                8 // timestamp bytes
        }

    override fun toString(): String {
        return "${this.key.hex()}@${this.timestamp}"
    }

}