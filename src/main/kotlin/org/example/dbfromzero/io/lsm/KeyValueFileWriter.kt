package org.example.dbfromzero.io.lsm

import org.example.dbfromzero.util.Bytes
import org.example.dbfromzero.util.PrefixIO
import java.io.BufferedOutputStream
import java.io.OutputStream

class KeyValueFileWriter : AutoCloseable {

    companion object {

        private const val DEFAULT_BUFFER_SIZE = 0x8000

    }

    private var outputStream: OutputStream

    @Transient
    private var closed: Boolean = false

    constructor(outputStream: OutputStream) {
        this.outputStream = if (outputStream is BufferedOutputStream) {
            outputStream
        } else {
            outputStream.buffered(DEFAULT_BUFFER_SIZE)
        }
    }

    fun append(key: Bytes, value: Bytes) {
        check(!this.closed){ "This KeyValueFileWriter has already been closed!" }
        PrefixIO.writeBytes(this.outputStream, key)
        PrefixIO.writeBytes(this.outputStream, value)
    }

    override fun close() {
        if(this.closed){
            return
        }
        this.closed = true
        this.outputStream.close()
    }
}