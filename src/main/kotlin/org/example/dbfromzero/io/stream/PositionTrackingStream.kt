package org.example.dbfromzero.io.stream

import java.io.BufferedOutputStream
import java.io.OutputStream

class PositionTrackingStream : BufferedOutputStream {

    companion object {

        private const val DEFAULT_BUFFER_SIZE = 0x8000

    }

    var position: Long = 0L

    constructor(outputStream: OutputStream, bufferSize: Int = DEFAULT_BUFFER_SIZE, startPosition: Long = 0L)
        : super(outputStream, bufferSize) {
        this.position = startPosition
    }

    constructor(outputStream: OutputStream, startPosition: Long) : this(outputStream, DEFAULT_BUFFER_SIZE, startPosition)

    override fun write(b: Int) {
        super.write(b)
        this.position += 1
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        super.write(b, off, len)
        this.position += len
    }

    // Note that write(byte[] b) is not override because that just calls write(byte[] b, int off, int len)

}