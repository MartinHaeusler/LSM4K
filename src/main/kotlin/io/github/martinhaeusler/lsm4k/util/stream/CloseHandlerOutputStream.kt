package io.github.martinhaeusler.lsm4k.util.stream

import java.io.OutputStream

/**
 * An [OutputStream] that supports a [closeHandler] which is called when the stream is [closed][close].
 */
class CloseHandlerOutputStream(
    private val out: OutputStream,
    private val closeHandler: () -> Unit,
) : OutputStream() {

    companion object {

        fun OutputStream.onClose(action: () -> Unit): OutputStream {
            return CloseHandlerOutputStream(this, action)
        }

    }

    private var closed = false

    override fun write(b: Int) {
        this.out.write(b)
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        this.out.write(b, off, len)
    }

    override fun write(b: ByteArray) {
        this.out.write(b)
    }

    override fun close() {
        if (closed) {
            return
        }
        closed = true
        this.closeHandler()
        this.out.close()
    }

    override fun toString(): String {
        return this.out.toString()
    }

}