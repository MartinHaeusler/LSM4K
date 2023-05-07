package org.chronos.chronostore.util.stream

import java.io.OutputStream

/**
 * An [OutputStream] which wraps a given [out], but when the wrapper is [closed][close], [out] is **not** called.
 *
 * This is useful in cases where some function always closes a stream and we want to use that function,
 * but we want our stream to remain open.
 */
class UnclosableOutputStream(
    private val out: OutputStream
): OutputStream() {

    companion object {

        fun OutputStream.unclosable(): OutputStream{
            return UnclosableOutputStream(this)
        }

    }

    override fun write(b: Int) {
        this.out.write(b)
    }

}