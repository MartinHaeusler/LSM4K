package org.lsm4k.test.util.bytes

import org.junit.jupiter.api.Test
import org.lsm4k.util.bytes.BasicBytes
import org.lsm4k.util.bytes.BytesBuffer
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo

class BytesBufferTest {

    @Test
    fun canTakeBytesFromBuffer() {
        val bytes = BasicBytes("Hello World!")

        val buffer = BytesBuffer(bytes)

        expectThat(buffer.remaining).isEqualTo(12)

        expectThat(buffer.takeBytes(5).asString()).isEqualTo("Hello")

        expectThat(buffer.remaining).isEqualTo(7)

        buffer.takeByte()

        expectThat(buffer.remaining).isEqualTo(6)

        expectThrows<IllegalArgumentException> {
            buffer.takeBytes(7)
        }

        expectThat(buffer.takeBytes(6).asString()).isEqualTo("World!")

        expectThat(buffer.remaining).isEqualTo(0)
    }

}