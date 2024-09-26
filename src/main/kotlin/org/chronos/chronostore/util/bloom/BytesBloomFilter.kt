package org.chronos.chronostore.util.bloom

import com.google.common.hash.BloomFilter
import org.chronos.chronostore.util.BloomFilterExtensions.toBytes
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.bytes.Bytes

@Suppress("UnstableApiUsage")
class BytesBloomFilter {

    companion object {

        fun readFrom(bytes: Bytes): BytesBloomFilter {
            val rawBloom = bytes.withInputStream { BloomFilter.readFrom(it, BytesFunnel) }
            return BytesBloomFilter(rawBloom, bytes.size)
        }

    }


    private val guavaBloomFilter: BloomFilter<Bytes>

    private var size: Int? = null

    constructor(expectedEntries: Long, falsePositiveRate: Double) {
        this.guavaBloomFilter = BloomFilter.create(BytesFunnel, expectedEntries, falsePositiveRate)
    }

    private constructor(rawBloom: BloomFilter<Bytes>, size: Int) {
        this.guavaBloomFilter = rawBloom
        this.size = size
    }

    fun put(element: Bytes) {
        this.guavaBloomFilter.put(element)
    }

    fun mightContain(element: Bytes): Boolean {
        return this.guavaBloomFilter.mightContain(element)
    }

    fun toBytes(): Bytes {
        return this.guavaBloomFilter.toBytes()
    }

    val sizeBytes: Long
        get() {
            val cached = this.size
            if (cached != null) {
                return cached.toLong()
            }
            val computedSize = this.toBytes().size
            this.size = computedSize
            return computedSize.toLong()
        }

}