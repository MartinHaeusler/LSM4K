package io.github.martinhaeusler.lsm4k.io.fileaccess

import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import java.io.EOFException

interface RandomFileAccessDriver : AutoCloseable {

    val fileSize: Long

    val filePath: String

    /**
     * Copies this driver.
     *
     * With the notable exception of the file a driver points to,
     * drivers are stateless (and thus easy to copy). However,
     * most driver implementations are still not thread-safe
     * because of interactions with the operating system. This
     * method allows to copy the driver, which allows another
     * thread to perform concurrent read access on the same
     * resource.
     *
     * This method is permitted even if `this` driver has
     * been closed. The copied instance will always be open,
     * but opening may fail due to I/O reasons (e.g. the
     * file might no longer exist).
     *
     * @return A new driver of the same type, targeting the
     * same file. The copy will need to be closed individually
     * and will have no inherent relationship with the original
     * driver, other than both of them targeting the same file.
     */
    fun copy(): RandomFileAccessDriver

    fun readBytes(offset: Long, bytesToRead: Int): Bytes {
        return this.readBytesOrNull(offset, bytesToRead)
            ?: throw EOFException("End of input has been reached while reading $bytesToRead bytes at offset ${offset}!")
    }

    fun readBytesOrNull(offset: Long, bytesToRead: Int): Bytes?

}