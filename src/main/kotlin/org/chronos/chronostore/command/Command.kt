package org.chronos.chronostore.command

import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianLong
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianLong
import org.chronos.chronostore.util.PrefixIO
import org.chronos.chronostore.util.StringExtensions.ellipsis
import org.chronos.chronostore.util.Timestamp
import java.io.ByteArrayOutputStream

class Command(
    val opCode: OpCode,
    val key: Bytes,
    val timestamp: Timestamp,
    val value: Bytes
) : Comparable<Command> {

    companion object {

        fun put(key: Bytes, timestamp: Timestamp, value: Bytes): Command {
            return Command(OpCode.PUT, key, timestamp, value)
        }

        fun put(key: String, timestamp: Timestamp, value: Bytes): Command {
            return put(Bytes(key.toByteArray()), timestamp, value)
        }

        fun put(key: Bytes, timestamp: Timestamp, value: String): Command {
            return put(key, timestamp, Bytes(value.toByteArray()))
        }

        fun put(key: String, timestamp: Timestamp, value: String): Command {
            return put(Bytes(key.toByteArray()), timestamp, Bytes(value.toByteArray()))
        }

        fun del(key: Bytes, timestamp: Timestamp): Command {
            return Command(OpCode.DEL, key, timestamp, Bytes.EMPTY)
        }

        fun del(key: String, timestamp: Timestamp): Command {
            return del(Bytes(key.toByteArray()), timestamp)
        }

        fun fromBytes(bytes: Bytes): Command {
            bytes.withInputStream { inputStream ->
                val opCode = OpCode.fromByte(inputStream.read())
                val key = PrefixIO.readBytes(inputStream)
                val timestamp = inputStream.readLittleEndianLong()
                val value = when (opCode) {
                    OpCode.PUT -> PrefixIO.readBytes(inputStream)
                    OpCode.DEL -> Bytes.EMPTY
                }
                return Command(
                    opCode = opCode,
                    key = key,
                    timestamp = timestamp,
                    value = value
                )
            }
        }

    }

    init {
        require(timestamp >= 0) { "Argument 'timestamp' must not be negative!" }
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

    fun toBytes(): Bytes {
        val length = byteSize

        val outputStream = ByteArrayOutputStream(1 + 8 + key.size + value.size)
        outputStream.write(this.opCode.byte.toInt())
        PrefixIO.writeBytes(outputStream, this.key)
        outputStream.writeLittleEndianLong(timestamp)
        when (this.opCode) {
            OpCode.PUT -> {
                PrefixIO.writeBytes(outputStream, this.value)
            }

            OpCode.DEL -> {
                /* No Op*/
            }
        }
        return Bytes(outputStream.toByteArray())
    }

    val byteSize: Int
        get() = when (this.opCode) {
            OpCode.PUT -> 1 /* opcode */ +
                4 /* key length*/ +
                key.size /* key bytes */ +
                8 /* timestamp */ +
                4 /* value length */ +
                value.size /* value bytes */

            OpCode.DEL -> 1 /* opcode */ +
                4 /* key length*/ +
                key.size /* key bytes */ +
                8 /* timestamp */
        }

    override fun compareTo(other: Command): Int {
        val keyCmp = this.key.compareTo(other.key)
        if (keyCmp != 0) {
            return keyCmp
        }
        val timeCmp = this.timestamp.compareTo(other.timestamp)
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
        if (timestamp != other.timestamp) return false
        return value == other.value
    }

    override fun hashCode(): Int {
        var result = opCode.hashCode()
        result = 31 * result + key.hashCode()
        result = 31 * result + timestamp.hashCode()
        result = 31 * result + value.hashCode()
        return result
    }

    override fun toString(): String {
        return when (this.opCode) {
            OpCode.PUT -> "PUT[${this.key.hex()}@${this.timestamp}: ${this.value.hex().ellipsis(32)}]"
            OpCode.DEL -> "DEL[${this.key.hex()}@${this.timestamp}]"
        }
    }

}