package org.example.dbfromzero.io.vfs

import java.io.OutputStream

interface VirtualReadWriteFile: VirtualFile {

    fun createAppendOutputStream(): OutputStream

    fun delete()

    fun createOverWriter(): OverWriter


    interface OverWriter {

        val outputStream: OutputStream

        fun commit()

        fun abort()

    }

}