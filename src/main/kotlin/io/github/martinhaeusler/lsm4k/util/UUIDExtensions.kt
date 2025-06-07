package io.github.martinhaeusler.lsm4k.util

import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import java.nio.ByteBuffer
import java.util.*


object UUIDExtensions {

    fun UUID.toBytes(): Bytes {
        return Bytes.wrap(this.toByteArray())
    }

    fun UUID.toByteArray(): ByteArray {
        val bb: ByteBuffer = ByteBuffer.wrap(ByteArray(16))
        bb.putLong(mostSignificantBits)
        bb.putLong(leastSignificantBits)
        return bb.array()
    }

    fun readUUIDFrom(bytes: Bytes): UUID {
        val byteBuffer = ByteBuffer.wrap(bytes.toSharedArrayUnsafe())
        val high = byteBuffer.getLong()
        val low = byteBuffer.getLong()
        return UUID(high, low)
    }

    fun readUUIDFrom(bytes: ByteArray): UUID {
        val byteBuffer = ByteBuffer.wrap(bytes)
        val high = byteBuffer.getLong()
        val low = byteBuffer.getLong()
        return UUID(high, low)
    }

}