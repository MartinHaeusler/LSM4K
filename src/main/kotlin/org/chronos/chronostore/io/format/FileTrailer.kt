package org.chronos.chronostore.io.format

import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianLong
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianLong
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


    fun writeTo(outputStream: OutputStream) {
        outputStream.writeLittleEndianLong(this.beginOfBlocks)
        outputStream.writeLittleEndianLong(this.beginOfIndexOfBlocks)
        outputStream.writeLittleEndianLong(this.beginOfMetadata)
    }


}