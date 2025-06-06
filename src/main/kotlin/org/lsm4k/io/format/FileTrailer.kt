package org.lsm4k.io.format

import org.lsm4k.util.IOExtensions.withInputStream
import org.lsm4k.util.LittleEndianExtensions.readLittleEndianLong
import org.lsm4k.util.LittleEndianExtensions.writeLittleEndianLong
import org.lsm4k.util.bytes.Bytes
import java.io.ByteArrayOutputStream
import java.io.OutputStream

class FileTrailer(
    val beginOfBlocks: Long,
    val beginOfIndexOfBlocks: Long,
    val beginOfMetadata: Long
) {

    companion object {

        fun readFrom(bytes: Bytes): FileTrailer {
            bytes.withInputStream { inputStream ->
                return FileTrailer(
                    beginOfBlocks = inputStream.readLittleEndianLong(),
                    beginOfIndexOfBlocks = inputStream.readLittleEndianLong(),
                    beginOfMetadata = inputStream.readLittleEndianLong(),
                )
            }
        }

        val SIZE_BYTES = Long.SIZE_BYTES * 3
    }

    fun asBytes(): Bytes {
        val baos = ByteArrayOutputStream()
        this.writeTo(baos)
        return Bytes.wrap(baos.toByteArray())
    }

    val sizeBytes: Int
        get(){
            return Long.SIZE_BYTES * 3
        }


    fun writeTo(outputStream: OutputStream) {
        outputStream.writeLittleEndianLong(this.beginOfBlocks)
        outputStream.writeLittleEndianLong(this.beginOfIndexOfBlocks)
        outputStream.writeLittleEndianLong(this.beginOfMetadata)
    }


}