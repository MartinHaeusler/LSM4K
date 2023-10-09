package org.chronos.chronostore.util

import org.chronos.chronostore.io.vfs.InputSource
import java.io.*

object IOExtensions {

    inline fun <T> InputSource.withInputStream(action: (InputStream) -> T): T {
        return this.createInputStream().use(action)
    }

    fun FileOutputStream.sync(file: File) {
        try {
            if (!this.channel.isOpen) {
                throw IOException("Cannot call sync() on '${file.path}': The FileOutputStream has already been closed!")
            }
            return this.fd.sync()
        } catch (e: SyncFailedException) {
            throw IOException("Could not call sync() on '${file.path}'!", e)
        }
    }

    @JvmStatic
    fun InputStream.readByte(): Byte? {
        val readValue = this.read()
        return if (readValue >= 0) {
            readValue.toByte()
        } else {
            null
        }
    }

    val File.size: Long
        get() {
            return when {
                this.isFile -> this.length()
                this.isDirectory -> this.computeDirectorySizeRecursively()
                else -> 0
            }
        }

    private fun File.computeDirectorySizeRecursively(): Long {
        return this.walkBottomUp().filter { it.isFile }.sumOf { it.length() }
    }
}