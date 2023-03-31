package org.chronos.chronostore.util

import com.google.common.hash.BloomFilter
import java.io.ByteArrayOutputStream

object BloomFilterExtensions {

    fun BloomFilter<*>.toBytes(): Bytes {
        val baos = ByteArrayOutputStream()
        this.writeTo(baos)
        return Bytes(baos.toByteArray())
    }

}