package org.lsm4k.model.command

import com.google.common.hash.Hasher
import org.lsm4k.model.command.Command.Companion.del
import org.lsm4k.model.command.Command.Companion.put
import org.lsm4k.model.command.Command.Companion.readFromBytes
import org.lsm4k.model.command.Command.Companion.readFromBytesBuffer
import org.lsm4k.util.IOExtensions.withInputStream
import org.lsm4k.util.LittleEndianExtensions.readLittleEndianLong
import org.lsm4k.util.LittleEndianExtensions.writeLittleEndianLong
import org.lsm4k.util.PrefixIO
import org.lsm4k.util.StringExtensions.ellipsis
import org.lsm4k.util.TSN
import org.lsm4k.util.bytes.Bytes
import org.lsm4k.util.bytes.Bytes.Companion.putBytes
import org.lsm4k.util.bytes.BytesBuffer
import org.lsm4k.util.hash.Hashable
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream

/**
 * A [Command] is a single, atomic change on a key-value pair.
 *
 * When a transaction gets committed, a list of [Command]s gets pushed into the Write Ahead Log and
 * subsequently applied to the LSM trees.
 *
 * The primary way to instantiate this class is via the static factory methods, e.g. [put] and [del].
 *
 * [Command]s are [Comparable] by first comparing the [key] and then by [tsn].
 *
 * [Command]s define a binary format which can be written via [writeToStream] and can be read
 * via [readFromBytes] or [readFromBytesBuffer].
 */
class Command(
    /** The [OpCode] of the command. Indicates the semantics of the change (Put or Delete). */
    val opCode: OpCode,
    /** The key of the key-value pair. */
    val key: Bytes,
    /** The [TSN] of the commit. */
    val tsn: TSN,
    /** The value of the key-value pair. Will be empty if [opCode] is [OpCode.DEL]. */
    val value: Bytes,
) : Comparable<Command>, Hashable {

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
                OpCode.PUT -> PrefixIO.readBytes(buffer)
                OpCode.DEL -> Bytes.EMPTY
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

        fun OutputStream.write(command: Command) {
            command.writeToStream(this)
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

        fun Hasher.putCommand(command: Command): Hasher {
            return command.hash(this)
        }

    }

    init {
        require(tsn >= 0) { "Argument 'tsn' must not be negative!" }
        require(key.isNotEmpty()) { "Argument 'key' must not be empty!" }
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

    fun getBinarySize(): Int {
        return 1 +           // opcode
            Int.SIZE_BYTES + // key length
            key.size +       // key content
            TSN.SIZE_BYTES + // Transaction Serial Number
            Int.SIZE_BYTES + // value length
            value.size       // value content
    }

    fun toBytes(): Bytes {
        val outputStream = ByteArrayOutputStream(this.getBinarySize())
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

    override fun hash(hasher: Hasher): Hasher {
        return hasher
            .putByte(this.opCode.byte)
            .putBytes(this.key)
            .putLong(this.tsn)
            .putBytes(this.value)
    }

    override fun toString(): String {
        return when (this.opCode) {
            OpCode.PUT -> "PUT[${this.key.hex()}@${this.tsn}: ${this.value.hex().ellipsis(32)}]"
            OpCode.DEL -> "DEL[${this.key.hex()}@${this.tsn}]"
        }
    }


}