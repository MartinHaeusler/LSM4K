package org.chronos.chronostore.util

import com.google.common.hash.BloomFilter
import org.chronos.chronostore.util.bytes.Bytes
import java.io.ByteArrayOutputStream

object BloomFilterExtensions {

    fun BloomFilter<*>.toBytes(): Bytes {
        val baos = ByteArrayOutputStream()
        this.writeTo(baos)
        return Bytes.wrap(baos.toByteArray())
    }

}