package org.chronos.chronostore.io.vfs

import java.io.OutputStream

interface VirtualReadWriteFile : VirtualFile {

    companion object {

        inline fun <T> VirtualReadWriteFile.withOverWriter(action: (OverWriter) -> T): T {
            return this.createOverWriter().use(action)
        }

    }

    fun createAppendOutputStream(): OutputStream

    fun delete()

    fun create()

    fun createOverWriter(): OverWriter

    fun deleteOverWriterFileIfExists()

    interface OverWriter : AutoCloseable {

        val outputStream: OutputStream

        fun commit()

        fun rollback()

        override fun close() {
            this.rollback()
        }
    }

}

