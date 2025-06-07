package io.github.martinhaeusler.lsm4k.test.util.bytes

import io.github.martinhaeusler.lsm4k.util.IOExtensions.readByte
import io.github.martinhaeusler.lsm4k.util.IOExtensions.withInputStream
import io.github.martinhaeusler.lsm4k.util.PrefixIO
import io.github.martinhaeusler.lsm4k.util.bytes.BasicBytes
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.bytes.SliceBytes
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.isEqualTo
import java.io.ByteArrayOutputStream
import java.util.*

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

        val woSlice = bytes.slice(1, 10).slice(5, 2)
        expectThat(woSlice){
            get { this.size }.isEqualTo(2)
            get { this.lastIndex }.isEqualTo(1)
            get { this.asString() }.isEqualTo("wo")
        }
    }

    @Test
    fun hashCodeAndEqualsWorks() {
        val bytes = BasicBytes("hello world!")

        val helloSlice = bytes.slice(0, 5)
        val helloBytes = BasicBytes("hello")

        expect {
            that(helloSlice.hashCode()).isEqualTo(helloBytes.hashCode())
            that(helloSlice).isEqualTo(helloBytes)
            that(helloBytes as Bytes).isEqualTo(helloSlice)
        }
    }

    @Test
    fun canIterateOverSliceOfBytes() {
        val rawArray = HexFormat.of().parseHex(
            "00000000000000014dc703ec94c74825b2645d0732435897" +
                "00000000000000000000000000000000000000000000" +
                "00000000000000000000000000000000000000000000" +
                "000000000000000000937e018b01000000000000"
        )
        val bytes = BasicBytes(rawArray)
        val slice = SliceBytes(rawArray, 0, rawArray.lastIndex)

        val bytesIterator = bytes.iterator()
        val sliceIterator = slice.iterator()

        repeat(rawArray.size) {
            val nextByte = bytesIterator.nextOrNull()
            val nextSlice = sliceIterator.nextOrNull()
            expectThat(nextSlice).isEqualTo(nextByte)
        }

        bytes.withInputStream { bytesInput ->
            slice.withInputStream { sliceInput ->
                repeat(rawArray.size) {
                    val bytesRead = bytesInput.readByte()
                    val sliceRead = sliceInput.readByte()
                    expectThat(sliceRead).isEqualTo(bytesRead)
                }
            }
        }
    }

    @Test
    fun canWriteSliceToOutputStream(){
        val rawBytes = BasicBytes("hello world")
        val slice = rawBytes.slice(2..7)
        val writtenBytes = ByteArrayOutputStream().use { baos ->
            PrefixIO.writeBytes(baos, slice)
            baos.flush()
            baos.toByteArray()
        }

        val readBytes = writtenBytes.inputStream().use { bais ->
            PrefixIO.readBytes(bais)
        }

        expectThat(readBytes.asString()).isEqualTo("llo wo")
    }

    private fun Byte.toCharacter(): Char {
        return Bytes.wrap(byteArrayOf(this)).asString()[0]
    }

    private fun <T> Iterator<T>.nextOrNull(): T? {
        return if (!this.hasNext()) {
            null
        } else {
            this.next()
        }
    }

}