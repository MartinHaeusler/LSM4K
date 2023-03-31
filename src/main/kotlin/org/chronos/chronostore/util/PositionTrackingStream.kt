package org.chronos.chronostore.util

import java.io.BufferedOutputStream
import java.io.OutputStream

class PositionTrackingStream(
    val outputStream: OutputStream,
    startPosition: Long = 0L
) : OutputStream() {

    var position: Long = startPosition

    override fun write(b: Int) {
        this.outputStream.write(b)
        this.position += 1
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        super.write(b, off, len)
        this.position += len
    }

    // Note that write(byte[] b) is not override because that just calls write(byte[] b, int off, int len)

}