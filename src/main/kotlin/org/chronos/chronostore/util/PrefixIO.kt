package org.chronos.chronostore.util

import org.chronos.chronostore.util.Bytes.Companion.write
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianIntOrNull
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianInt
import java.io.EOFException
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
        return Bytes(array)
    }

    fun readBytesOrNull(inputStream: InputStream): Bytes? {
        return try {
            this.readBytes(inputStream)
        } catch (e: Exception) {
            null
        }
    }

}