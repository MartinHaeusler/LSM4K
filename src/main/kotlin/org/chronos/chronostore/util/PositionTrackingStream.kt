package org.chronos.chronostore.util

import java.io.OutputStream

class PositionTrackingStream(
    val outputStream: OutputStream,
    startPosition: Long = 0L
) : OutputStream() {

    var position: Long = startPosition
        private set

    override fun write(b: Int) {
        this.outputStream.write(b)
        this.position += 1
    }

    override fun flush() {
        this.outputStream.flush()
    }

    override fun close() {
        this.outputStream.close()
    }

}