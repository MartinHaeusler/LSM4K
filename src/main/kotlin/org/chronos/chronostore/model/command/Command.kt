package org.chronos.chronostore.model.command

import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianLong
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianLong
import org.chronos.chronostore.util.PrefixIO
import org.chronos.chronostore.util.StringExtensions.ellipsis
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.bytes.BytesBuffer
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream

class Command(
    val opCode: OpCode,
    val key: Bytes,
    val tsn: TSN,
    val value: Bytes,
) : Comparable<Command> {

    companion object {

        fun put(key: Bytes, tsn: TSN, value: Bytes): Command {
            return Command(OpCode.PUT, key, tsn, value)
        }

        fun put(key: String, tsn: TSN, value: Bytes): Command {
            return put(Bytes.wrap(key.toByteArray()), tsn, value)
        }

        fun put(key: Bytes, tsn: TSN, value: String): Command {
            return put(key, tsn, Bytes.wrap(value.toByteArray()))
        }

        fun put(key: String, tsn: TSN, value: String): Command {
            return put(Bytes.wrap(key.toByteArray()), tsn, Bytes.wrap(value.toByteArray()))
        }

        fun del(key: Bytes, tsn: TSN): Command {
            return Command(OpCode.DEL, key, tsn, Bytes.EMPTY)
        }

        fun del(key: String, tsn: TSN): Command {
            return del(Bytes.wrap(key.toByteArray()), tsn)
        }

        fun readFromBytesBuffer(buffer: BytesBuffer): Command? {
            val firstByte = buffer.takeByte()
            if (firstByte < 0) {
                // end of input
                return null
            }
            val opCode = OpCode.fromByte(firstByte)
            val key = PrefixIO.readBytes(buffer)
            val tsn = buffer.takeLittleEndianLong()
            val value = when (opCode) {
                OpCode.PUT -> {
                    PrefixIO.readBytes(buffer)
                }

                OpCode.DEL -> {
                    Bytes.EMPTY
                }
            }
            return Command(
                opCode = opCode,
                key = key,
                tsn = tsn,
                value = value
            )
        }

        fun readFromBytes(bytes: Bytes): Command {
            bytes.withInputStream { inputStream ->
                return readFromStream(inputStream)
            }
        }

        fun readFromStream(inputStream: InputStream): Command {
            return readFromStreamOrNull(inputStream)
                ?: throw IllegalStateException("Cannot read Command from input stream: it has no more data!")
        }

        fun readFromStreamOrNull(inputStream: InputStream): Command? {
            val firstByte = inputStream.read()
            if (firstByte < 0) {
                // end of input
                return null
            }
            val opCode = OpCode.fromByte(firstByte)
            val key = PrefixIO.readBytes(inputStream)
            val tsn = inputStream.readLittleEndianLong()
            val value = when (opCode) {
                OpCode.PUT -> PrefixIO.readBytes(inputStream)
                OpCode.DEL -> Bytes.EMPTY
            }
            return Command(
                opCode = opCode,
                key = key,
                tsn = tsn,
                value = value
            )
        }

    }

    init {
        require(tsn >= 0) { "Argument 'tsn' must not be negative!" }
        require(key.isNotEmpty()) { "Argument 'key' must not be empty!" }
    }

    enum class OpCode(val byte: Byte) {

        PUT(0b00000001),

        DEL(0b00000010);

        companion object {

            fun fromByte(byte: Int): OpCode {
                return fromByte(byte.toByte())
            }

            fun fromByte(byte: Byte): OpCode {
                return when (byte) {
                    PUT.byte -> PUT
                    DEL.byte -> DEL
                    else -> throw IllegalArgumentException("Unknown OpCode: $byte")
                }
            }

        }

    }

    val isDeletion: Boolean
        get() = this.opCode == OpCode.DEL

    val isInsertOrUpdate: Boolean
        get() = this.opCode == OpCode.PUT

    fun writeToStream(outputStream: OutputStream) {
        outputStream.write(this.opCode.byte.toInt())
        PrefixIO.writeBytes(outputStream, this.key)
        outputStream.writeLittleEndianLong(this.tsn)
        when (this.opCode) {
            OpCode.PUT -> {
                PrefixIO.writeBytes(outputStream, this.value)
            }

            OpCode.DEL -> {
                /* No Op*/
            }
        }
    }

    fun toBytes(): Bytes {
        val outputStream = ByteArrayOutputStream(1 + 8 + key.size + value.size)
        writeToStream(outputStream)
        return Bytes.wrap(outputStream.toByteArray())
    }

    val byteSize: Int
        get() = when (this.opCode) {
            OpCode.PUT -> 1 /* opcode */ +
                4 /* key length*/ +
                key.size /* key bytes */ +
                8 /* tsn */ +
                4 /* value length */ +
                value.size /* value bytes */

            OpCode.DEL -> 1 /* opcode */ +
                4 /* key length*/ +
                key.size /* key bytes */ +
                8 /* tsn */
        }

    val keyAndTSN: KeyAndTSN
        get() = KeyAndTSN(this.key, this.tsn)

    override fun compareTo(other: Command): Int {
        val keyCmp = this.key.compareTo(other.key)
        if (keyCmp != 0) {
            return keyCmp
        }
        val timeCmp = this.tsn.compareTo(other.tsn)
        if (timeCmp != 0) {
            return timeCmp
        }
        // the rest doesn't REALLY make sense and only exists
        // for compatibility with equals() and hashCode().
        val opCodeCmp = this.opCode.compareTo(other.opCode)
        if (opCodeCmp != 0) {
            return opCodeCmp
        }
        return this.value.compareTo(other.value)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Command

        if (opCode != other.opCode) return false
        if (key != other.key) return false
        if (tsn != other.tsn) return false
        return value == other.value
    }

    override fun hashCode(): Int {
        var result = opCode.hashCode()
        result = 31 * result + key.hashCode()
        result = 31 * result + tsn.hashCode()
        result = 31 * result + value.hashCode()
        return result
    }

    override fun toString(): String {
        return when (this.opCode) {
            OpCode.PUT -> "PUT[${this.key.hex()}@${this.tsn}: ${this.value.hex().ellipsis(32)}]"
            OpCode.DEL -> "DEL[${this.key.hex()}@${this.tsn}]"
        }
    }

}