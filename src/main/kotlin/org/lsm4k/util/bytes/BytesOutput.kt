package org.lsm4k.util.bytes

interface BytesOutput {

    fun write(bytes: ByteArray)

    fun write(bytes: ByteArray, offset: Int, length: Int)

}