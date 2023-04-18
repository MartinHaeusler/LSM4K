package org.chronos.chronostore.util

import java.io.PushbackInputStream

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



}