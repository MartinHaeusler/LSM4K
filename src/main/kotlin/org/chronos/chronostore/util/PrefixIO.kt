package org.chronos.chronostore.util

import org.chronos.chronostore.api.exceptions.TruncatedInputException
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianInt
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.bytes.Bytes.Companion.writeBytesWithoutSize
import org.chronos.chronostore.util.bytes.BytesBuffer
import java.io.InputStream
import java.io.OutputStream

object PrefixIO {

    fun writeBytes(outputStream: OutputStream, bytes: Bytes) {
        outputStream.writeLittleEndianInt(bytes.size)
        outputStream.writeBytesWithoutSize(bytes)
    }

    fun readBytes(inputStream: InputStream): Bytes {
        val length = inputStream.readLittleEndianInt()
        if(length <= 0){
            // don't attempt to read anything if the length is zero,
            // this may throw an exception if the zero-length is the
            // last byte in the stream!
            return Bytes.EMPTY
        }
        val array = inputStream.readNBytes(length)
        if(array.size < length){
            // not enough bytes in the stream
            throw TruncatedInputException("Attempted to read ${length} bytes but got only ${array.size}!")
        }
        return Bytes.wrap(array)
    }

    fun readBytes(buffer: BytesBuffer): Bytes {
        val length = buffer.takeLittleEndianInt()
        if(length <= 0){
            // don't attempt to read anything if the length is zero,
            // this may throw an exception if the zero-length is the
            // last byte in the buffer!
            return Bytes.EMPTY
        }
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