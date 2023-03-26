package org.example.dbfromzero.util

import org.example.dbfromzero.util.LittleEndianExtensions.readLittleEndianInt
import org.example.dbfromzero.util.LittleEndianExtensions.writeLittleEndianInt
import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream

object PrefixIO {

    fun writeBytes(outputStream: OutputStream, bytes: Bytes) {
        outputStream.writeLittleEndianInt(bytes.size)
        bytes.writeToStream(outputStream)
    }

    fun readBytes(inputStream: InputStream): Bytes {
        val length = inputStream.readLittleEndianInt()
        val array = ByteArray(length)
        val read = inputStream.read(array)
        if(read != length){
            throw EOFException("Unexpected end-of-input: expected $length bytes, but got $read!")
        }
        return Bytes(array)
    }

}