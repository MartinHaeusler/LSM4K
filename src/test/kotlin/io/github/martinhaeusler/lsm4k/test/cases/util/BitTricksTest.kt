package io.github.martinhaeusler.lsm4k.test.cases.util

import io.github.martinhaeusler.lsm4k.util.IOExtensions.withInputStream
import io.github.martinhaeusler.lsm4k.util.bits.BitTricks.readStableLong
import io.github.martinhaeusler.lsm4k.util.bits.BitTricks.writeStableLong
import io.github.martinhaeusler.lsm4k.util.bytes.BasicBytes
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isLessThan
import java.io.ByteArrayOutputStream

class BitTricksTest {

    @Test
    fun canReadAndWriteStableLongs(){
        val longs = mutableListOf(Long.MIN_VALUE, Long.MAX_VALUE, 0, -1, +1, -2, +2, -10, +10, -42, +42, Long.MAX_VALUE / 2, Long.MIN_VALUE / 2)
        longs.sort()

        for ((lower, upper) in longs.windowed(size = 2)) {
            expectThat(lower).isLessThan(upper)

            val lowerBytes = BasicBytes(lower.toStableBytes())
            val upperBytes = BasicBytes(upper.toStableBytes())

            expectThat(lowerBytes).isLessThan(upperBytes)

            expectThat(lowerBytes.toStableLong()).isEqualTo(lower)
            expectThat(upperBytes.toStableLong()).isEqualTo(upper)
        }

    }


    private fun Long.toStableBytes(): ByteArray {
        return ByteArrayOutputStream().use { out -> out.writeStableLong(this); out.toByteArray() }
    }

    private fun Bytes.toStableLong(): Long {
        return this.withInputStream { input -> input.readStableLong() }
    }

}