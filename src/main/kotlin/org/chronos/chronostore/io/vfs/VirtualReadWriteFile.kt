package org.chronos.chronostore.io.vfs

import java.io.OutputStream

interface VirtualReadWriteFile : VirtualFile {

    companion object {

        inline fun <T> VirtualReadWriteFile.withOverWriter(action: (OverWriter) -> T): T {
            return this.createOverWriter().use(action)
        }

    }

    fun <T> append(action: (OutputStream) -> T): T

    fun delete()

    fun deleteIfExists(): Boolean

    fun create(): VirtualReadWriteFile

    fun createOverWriter(): OverWriter

    fun deleteOverWriterFileIfExists()

    /**
     * Truncates the file, i.e. deletes all data after the first [bytesToKeep] bytes.
     *
     * This operation is only valid for files which currently **exist**. Calling this method on a non-existing file will
     * throw an [IllegalStateException].
     *
     * @param bytesToKeep  The number of bytes to keep in the file. May be zero (to clear the file entirely) but must not be negative.
     *                     All bytes after this limit will be deleted. If this value is greater than the size of the file, this
     *                     operation is a no-op.
     */
    fun truncateAfter(bytesToKeep: Long)


    interface OverWriter : AutoCloseable {

        val outputStream: OutputStream

        fun commit()

        fun rollback()

        override fun close() {
            this.rollback()
        }
    }

}

