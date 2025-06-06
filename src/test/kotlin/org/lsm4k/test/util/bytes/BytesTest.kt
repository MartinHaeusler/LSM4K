package org.lsm4k.test.util.bytes

import org.junit.jupiter.api.Test
import org.lsm4k.util.bytes.Bytes
import strikt.api.expect
import strikt.assertions.isEqualTo
import strikt.assertions.isGreaterThan
import strikt.assertions.isLessThan

class BytesTest {

    @Test
    fun canCompareBytesLexicographically() {
        val b1 = Bytes.of(
            0b001,
            0b010,
            0b110,
        )

        val b2 = Bytes.of(
            0b001,
            0b010,
            0b111,
        )

        val b3 = Bytes.of(
            0b001,
            0b010,
        )

        expect {
            that(b1.compareTo(b1)).isEqualTo(0)
            that(b2.compareTo(b2)).isEqualTo(0)
            that(b3.compareTo(b3)).isEqualTo(0)

            that(b1.compareTo(b2)).isLessThan(0)
            that(b2.compareTo(b1)).isGreaterThan(0)
            that(b2.compareTo(b3)).isGreaterThan(0)
            that(b1.compareTo(b3)).isGreaterThan(0)
            that(b3.compareTo(b1)).isLessThan(0)
            that(b3.compareTo(b1)).isLessThan(0)
        }
    }

}