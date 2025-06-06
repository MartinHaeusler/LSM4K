package org.lsm4k.util

import com.google.common.io.CountingInputStream
import java.io.InputStream
import java.io.PushbackInputStream
import java.security.DigestInputStream
import java.security.MessageDigest
import java.util.zip.CheckedInputStream
import java.util.zip.Checksum

object StreamExtensions {

    fun PushbackInputStream.hasNext(): Boolean {
        val readByte = this.read()
        return if (readByte < 0) {
            false
        } else {
            this.unread(readByte)
            true
        }
    }

    fun InputStream.checked(checksum: Checksum): CheckedInputStream {
        return CheckedInputStream(this,checksum)
    }

    fun InputStream.digest(digest: MessageDigest): DigestInputStream {
        return DigestInputStream(this, digest)
    }

    fun InputStream.pushback(): PushbackInputStream {
        return PushbackInputStream(this)
    }

    fun InputStream.byteCounting(): CountingInputStream {
        return CountingInputStream(this)
    }

}