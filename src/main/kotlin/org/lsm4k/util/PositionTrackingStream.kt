package org.lsm4k.util

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

    override fun write(b: ByteArray) {
        // performance optimization: we forward the bulk-write API directly
        // here. The default implementation uses the write(Int) method repeatedly,
        // but doing so makes any buffering in the output useless and the bytes
        // will arrive one by one in the destination stream. By explicitly overriding
        // this method. we guarantee that buffering will have the desired effect.
        this.outputStream.write(b)
        this.position += b.size
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        // performance optimization: we forward the bulk-write API directly
        // here. The default implementation uses the write(Int) method repeatedly,
        // but doing so makes any buffering in the output useless and the bytes
        // will arrive one by one in the destination stream. By explicitly overriding
        // this method. we guarantee that buffering will have the desired effect.
        this.outputStream.write(b, off, len)
        this.position += len
    }

    override fun flush() {
        this.outputStream.flush()
    }

    override fun close() {
        this.outputStream.close()
    }

}