package io.github.martinhaeusler.lsm4k.test.cases.command

import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnels
import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.util.bytes.BasicBytes
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes.Companion.mightContain
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes.Companion.put
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes.Companion.writeBytesWithoutSize
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isTrue
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class CommandTest {

    @Test
    fun canSerializeAndDeserializePutCommand() {
        val cmd = Command.put("foo", 123456, "bar")
        val serialized = cmd.toBytes()
        val deserialized = Command.readFromBytes(serialized)
        expectThat(deserialized).isEqualTo(cmd)
    }

    @Test
    fun canSerializeAndDeserializeDeleteCommand() {
        val cmd = Command.del("foo", 123456)
        val serialized = cmd.toBytes()
        val deserialized = Command.readFromBytes(serialized)
        expectThat(deserialized).isEqualTo(cmd)
    }

    @Test
    fun canSerializeAndDeserializeCommandWithMaxTimestamp() {
        val cmd = Command.put("foo", Long.MAX_VALUE, "bar")
        val serialized = cmd.toBytes()
        val deserialized = Command.readFromBytes(serialized)
        expectThat(deserialized).isEqualTo(cmd)
    }

    @Test
    fun canSerializeAndDeserializeMultipleCommands(){
        val commands = mutableListOf(
            Command.put("foo", 123456, "bar"),
            Command.del("foo", 123500),
            Command.put("foo", 123512, "baz"),
            Command.put("hello", 123456, "world")
        )
        val out = ByteArrayOutputStream()
        for(command in commands){
            out.writeBytesWithoutSize(command.toBytes())
        }
        val serialized = out.toByteArray()
        val input = ByteArrayInputStream(serialized)
        val deserialized = mutableListOf<Command>()
        while(input.available() > 0){
            deserialized += Command.readFromStream(input)
        }
        expectThat(deserialized).containsExactly(commands)
    }

    @Test
    fun canSortFirsByKeyThenByTimestamp() {
        val commands = listOf(
            Command.put("foo", 123456, "bar"),
            Command.put("foo", 123457, "bar"),
            Command.put("foo", 123455, "bar"),
            Command.put("foo1", 23456, "bar"),
            Command.put("fo", 123456, "bar")
        )
        val sorted = commands.shuffled().sorted()
        expectThat(sorted).containsExactly(
            Command.put("fo", 123456, "bar"),
            Command.put("foo", 123455, "bar"),
            Command.put("foo", 123456, "bar"),
            Command.put("foo", 123457, "bar"),
            Command.put("foo1", 23456, "bar"),
        )
    }

    @Test
    fun canComputeByteSize() {
        val commands = listOf(
            Command.put("foo", 123456, "bar"),
            Command.put("foo", 123457, "bar"),
            Command.put("foo", 123455, "bar"),
            Command.put("foo1", 23456, "bar"),
            Command.put("fo", 123456, "bar"),
            Command.del("bar", 12),
        )

        for(command in commands){
            assertEquals(command.toBytes().size, command.byteSize){
                "Expected command '${command}' to have a byteSize of ${command.toBytes().size}, but got ${command.byteSize}!"
            }
        }
    }

    @Test
    fun canCreateBloomFilterForKeys(){
        val bloomFilter = BloomFilter.create(
            Funnels.byteArrayFunnel(),
            6,
            0.01
        )

        val commands = listOf(
            Command.put("foo", 123456, "bar"),
            Command.put("foo", 123457, "bar"),
            Command.put("foo", 123455, "bar"),
            Command.put("foo1", 23456, "bar"),
            Command.put("fo", 123456, "bar"),
            Command.del("bar", 12),
        )

        commands.asSequence().map { it.key }.forEach { bloomFilter.put(it) }

        val byteArrayOutputStream = ByteArrayOutputStream()
        bloomFilter.writeTo(byteArrayOutputStream)
        val bloomBytes = byteArrayOutputStream.toByteArray()

        val deserializedBloom = BloomFilter.readFrom(ByteArrayInputStream(bloomBytes), Funnels.byteArrayFunnel())

        expectThat(deserializedBloom){
            get { mightContain(BasicBytes("foo")) }.isTrue()
            get { mightContain(BasicBytes("foo1")) }.isTrue()
            get { mightContain(BasicBytes("fo")) }.isTrue()
            get { mightContain(BasicBytes("bar")) }.isTrue()
            get { mightContain(BasicBytes("apple")) }.isFalse()
            get { mightContain(BasicBytes("bullshit")) }.isFalse()
        }
    }

}