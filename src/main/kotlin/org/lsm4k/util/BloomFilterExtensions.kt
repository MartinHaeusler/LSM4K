package org.lsm4k.util

import com.google.common.hash.BloomFilter
import org.lsm4k.util.bytes.Bytes
import java.io.ByteArrayOutputStream

object BloomFilterExtensions {

    fun BloomFilter<*>.toBytes(): Bytes {
        val baos = ByteArrayOutputStream()
        this.writeTo(baos)
        return Bytes.wrap(baos.toByteArray())
    }

}