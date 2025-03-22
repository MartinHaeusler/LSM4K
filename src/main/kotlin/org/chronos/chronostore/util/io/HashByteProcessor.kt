package org.chronos.chronostore.util.io

import com.google.common.hash.HashCode
import com.google.common.hash.Hasher
import com.google.common.io.ByteProcessor

/**
 * A simple Guava [ByteProcessor] implementation which forwards all data to the given [hasher] and finally produces the [HashCode].
 */
class HashByteProcessor(
    val hasher: Hasher,
) : ByteProcessor<HashCode> {

    override fun processBytes(buf: ByteArray, off: Int, len: Int): Boolean {
        hasher.putBytes(buf, off, len)
        return true
    }

    override fun getResult(): HashCode {
        return hasher.hash()
    }

}