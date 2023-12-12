package org.chronos.chronostore.util.bytes

class TrackingBytesOutput : BytesOutput {

    var lastBytes: ByteArray? = null
    var lastOffset: Int? = null
    var lastLength: Int? = null

    override fun write(bytes: ByteArray) {
        this.lastBytes = bytes
        this.lastOffset = null
        this.lastLength = null
    }

    override fun write(bytes: ByteArray, offset: Int, length: Int) {
        this.lastBytes = bytes
        this.lastOffset = offset
        this.lastLength = length
    }

}