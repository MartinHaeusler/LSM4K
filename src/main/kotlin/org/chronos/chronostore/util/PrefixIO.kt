package org.chronos.chronostore.util

import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianIntOrNull
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianInt
import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream

object PrefixIO {

    fun writeBytes(outputStream: OutputStream, bytes: Bytes) {
        outputStream.writeLittleEndianInt(bytes.size)
        bytes.writeToStream(outputStream)
    }

    fun readBytes(inputStream: InputStream): Bytes {
        return readBytesOrNull(inputStream)
            ?: throw EOFException("Cannot read prefixed byte array due to unexpected end-of-input!")
    }

    fun readBytesOrNull(inputStream: InputStream): Bytes? {
        val length = inputStream.readLittleEndianIntOrNull()
            ?: return null
        val array = ByteArray(length)
        val read = inputStream.read(array)
        if (read != length) {
            return null
        }
        return Bytes(array)
    }

}