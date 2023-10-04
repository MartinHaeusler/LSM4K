package org.chronos.chronostore.test.util.bytes

import org.chronos.chronostore.util.bytes.BasicBytes
import org.chronos.chronostore.util.bytes.Bytes
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.isEqualTo

class SliceBytesTest {

    @Test
    fun canIterateOverBasicBytes() {
        val bytes = BasicBytes("hello world!")
        val iterator = bytes.iterator()

        val characters = iterator.asSequence().map { it.toCharacter() }.toList()
        expectThat(characters).containsExactly('h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', '!')
    }

    @Test
    fun canProduceSlice() {
        val bytes = BasicBytes("hello world!")

        val helloSlice = bytes.slice(0, 5)
        expectThat(helloSlice) {
            get { this.size }.isEqualTo(5)
            get { this.lastIndex }.isEqualTo(4)
            get { this.asString() }.isEqualTo("hello")
            get { this.asSequence().map { it.toCharacter() }.toList() }.containsExactly('h', 'e', 'l', 'l', 'o')
        }

        val worldSlice = bytes.slice(6)
        expectThat(worldSlice) {
            get { this.size }.isEqualTo(6)
            get { this.lastIndex }.isEqualTo(5)
            get { this.asString() }.isEqualTo("world!")
            get { this.asSequence().map { it.toCharacter() }.toList() }.containsExactly('w', 'o', 'r', 'l', 'd', '!')
        }
    }

    @Test
    fun canProduceSliceOfSlice() {
        val bytes = BasicBytes("hello world!")

        val helloSlice = bytes.slice(0, 5)
        expectThat(helloSlice) {
            get { this.size }.isEqualTo(5)
            get { this.lastIndex }.isEqualTo(4)
            get { this.asString() }.isEqualTo("hello")
        }

        val llSlice = helloSlice.slice(2, 2)
        expectThat(llSlice) {
            get { this.size }.isEqualTo(2)
            get { this.lastIndex }.isEqualTo(1)
            get { this.asString() }.isEqualTo("ll")
        }
    }

    @Test
    fun hashCodeAndEqualsWorks(){
        val bytes = BasicBytes("hello world!")

        val helloSlice = bytes.slice(0, 5)
        val helloBytes = BasicBytes("hello")

        expect{
            that(helloSlice.hashCode()).isEqualTo(helloBytes.hashCode())
            that(helloSlice).isEqualTo(helloBytes)
            that(helloBytes as Bytes).isEqualTo(helloSlice)
        }
    }

    private fun Byte.toCharacter(): Char {
        return Bytes.wrap(byteArrayOf(this)).asString()[0]
    }

}