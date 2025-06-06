package org.lsm4k.util.stream

import java.io.OutputStream

/**
 * An [OutputStream] which wraps a given [out], but when the wrapper is [closed][close], [out] is **not** called.
 *
 * This is useful in cases where some function always closes a stream and we want to use that function,
 * but we want our stream to remain open.
 */
class UnclosableOutputStream(
    private val out: OutputStream,
) : OutputStream() {

    companion object {

        fun OutputStream.unclosable(): OutputStream {
            return UnclosableOutputStream(this)
        }

    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        this.out.write(b, off, len)
    }

    override fun write(b: ByteArray) {
        // performance optimization: we forward the bulk-write API directly
        // here. The default implementation uses the write(Int) method repeatedly,
        // but doing so makes any buffering in the output useless and the bytes
        // will arrive one by one in the destination stream. By explicitly overriding
        // this method. we guarantee that buffering will have the desired effect.
        this.out.write(b)
    }

    override fun write(b: Int) {
        // performance optimization: we forward the bulk-write API directly
        // here. The default implementation uses the write(Int) method repeatedly,
        // but doing so makes any buffering in the output useless and the bytes
        // will arrive one by one in the destination stream. By explicitly overriding
        // this method. we guarantee that buffering will have the desired effect.
        this.out.write(b)
    }

}