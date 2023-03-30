package org.chronos.chronostore.io.fileaccess

import org.chronos.chronostore.util.Bytes
import java.io.File
import java.io.RandomAccessFile

/**
 * A [RandomFileAccessDriver] based on a [RandomAccessFile].
 */
class RandomAccessFileDriver(
    val file: File
) : RandomFileAccessDriver {

    val randomAccessFile: RandomAccessFile = RandomAccessFile(this.file, "r")

    override val size: Long by lazy {
        // we assume that the file size doesn't change
        // while we're reading, so we can cache it.
        this.randomAccessFile.length()
    }

    override fun readBytesOrNull(offset: Long, bytesToRead: Int): Bytes? {
        require(offset >= 0) { "Argument 'offset' must not be negative, but got: ${offset}!" }
        require(bytesToRead >= 0) { "Argument 'bytesToRead' must not be negative, but got: ${offset}!" }
        if (offset > this.size) {
            // offset is outside the file!
            return null
        }
        // since the "read" method only supports integer offsets,
        // we have to perform a "seek" first. This will store an
        // implicit offset in the RandomAccessFile, and the integer
        // offset specified in "read" will be relative to this position.
        this.randomAccessFile.seek(offset)

        val buffer = ByteArray(bytesToRead)
        var n = 0
        do {
            val count = this.randomAccessFile.read(buffer, n, bytesToRead - n)
            if (count < 0) {
                // end of file reached
                return null
            }
            n += count
        } while (n < bytesToRead)
        return Bytes(buffer)
    }

    override fun close() {
        this.randomAccessFile.close()
    }

}