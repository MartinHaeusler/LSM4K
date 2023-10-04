package org.chronos.chronostore.util

import org.chronos.chronostore.util.bytes.Bytes.Companion.write
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianInt
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.bytes.BytesBuffer
import java.io.InputStream
import java.io.OutputStream

object PrefixIO {

    fun writeBytes(outputStream: OutputStream, bytes: Bytes) {
        outputStream.writeLittleEndianInt(bytes.size)
        outputStream.write(bytes)
    }

    fun readBytes(inputStream: InputStream): Bytes {
        val length = inputStream.readLittleEndianInt()
        val array = inputStream.readNBytes(length)
        return Bytes.wrap(array)
    }

    fun readBytes(buffer: BytesBuffer): Bytes {
        val length = buffer.takeLittleEndianInt()
        return buffer.takeBytes(length)
    }

    fun readBytesOrNull(inputStream: InputStream): Bytes? {
        return try {
            this.readBytes(inputStream)
        } catch (e: Exception) {
            null
        }
    }

}