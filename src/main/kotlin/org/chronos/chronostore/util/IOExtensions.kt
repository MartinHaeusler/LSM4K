package org.chronos.chronostore.util

import org.chronos.chronostore.io.vfs.InputSource
import java.io.FileOutputStream
import java.io.InputStream

object IOExtensions {

    inline fun <T> InputSource.withInputStream(action: (InputStream) -> T): T {
        return this.createInputStream().use(action)
    }

    fun FileOutputStream.sync() {
        return this.fd.sync()
    }

    @JvmStatic
    fun InputStream.readByte(): Byte? {
        val readValue = this.read()
        return if (readValue >= 0) {
            readValue.toByte()
        }else{
            null
        }
    }

}