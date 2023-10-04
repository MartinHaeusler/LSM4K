package org.chronos.chronostore.util.bytes

class BytesBuffer(
    private val bytes: Bytes
) {

    private var position = 0

    val remaining: Int
        get() = bytes.lastIndex - position

    fun takeByte(): Byte {
        require(remaining > 0){ "Cannot take 1 Byte from BytesBuffer - no bytes remain!" }
        val result = this.bytes[position]
        position++
        return result
    }

    fun takeBytes(bytes: Int): Bytes {
        require(bytes <= remaining) { "Cannot take ${bytes} Bytes from BytesBuffer - only ${remaining} bytes remain!" }
        val result = this.bytes.slice(position, bytes)
        position += bytes
        return result
    }

    fun skipBytes(bytes: Int) {
        require(bytes <= remaining) { "Cannot take ${bytes} from BytesBuffer - only ${remaining} bytes remain!" }
        position += bytes
    }

    fun takeLittleEndianInt(): Int {
        return this.takeBytes(Int.SIZE_BYTES).readLittleEndianInt(0)
    }

    fun takeLittleEndianLong(): Long {
        return this.takeBytes(Long.SIZE_BYTES).readLittleEndianLong(0)
    }

}