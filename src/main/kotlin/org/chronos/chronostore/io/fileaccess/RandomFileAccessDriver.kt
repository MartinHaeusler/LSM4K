package org.chronos.chronostore.io.fileaccess

import org.chronos.chronostore.util.Bytes
import java.io.EOFException

interface RandomFileAccessDriver: AutoCloseable {

    val size: Long

    fun readBytes(offset: Long, bytesToRead: Int): Bytes {
        return this.readBytesOrNull(offset, bytesToRead)
            ?: throw EOFException("End of input has been reached while reading $bytesToRead bytes at offset ${offset}!")
    }

    fun readBytesOrNull(offset: Long, bytesToRead: Int): Bytes?

}