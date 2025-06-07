package io.github.martinhaeusler.lsm4k.wal.format

import com.google.common.hash.Hasher
import io.github.martinhaeusler.lsm4k.api.exceptions.WriteAheadLogCorruptedException
import io.github.martinhaeusler.lsm4k.impl.annotations.PersistentClass
import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.model.command.Command.Companion.putCommand
import io.github.martinhaeusler.lsm4k.model.command.Command.Companion.write
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.readLittleEndianInt
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.writeLittleEndianInt
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.StoreId.Companion.putStoreId
import io.github.martinhaeusler.lsm4k.util.StoreId.Companion.write
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.wal.format.TransactionCommandEntry.Companion.TYPE_BYTE
import io.github.martinhaeusler.lsm4k.wal.format.WALEntry.Companion.HASH_FUNCTION
import java.io.InputStream
import java.io.OutputStream

/**
 * A [WALEntry] which signifies a single change to a key-value pair in a single store.
 *
 * Every individual [TransactionCommandEntry] adheres to the following binary schema when serialized
 * (excluding the type byte and hash which is common to all [WALEntry] classes):
 *
 * ```
 *       5 Byte             4 Byte       variable length    variable length
 * +-----------------+-----------------+-----------------+------------------+
 * | WALEntry header |  StoreId Length | StoreId content | Command Content |
 * +-----------------+-----------------+-----------------+-----------------+
 * ```
 *
 * - `WALEntry header` is the common header of all entries described in [WALEntry].
 * - `StoreId Length` is a little-endian integer which indicates the (binary) length of the [storeId].
 * - `StoreId Content` is the UTF-8 representation of the [storeId].
 * - `Command Content` is the serialized form of the [command].
 */
@PersistentClass(format = PersistentClass.Format.BINARY, details = "Used in WAL.")
data class TransactionCommandEntry(
    val storeId: StoreId,
    val command: Command,
) : WALEntry {

    companion object {

        /**
         * A [Byte] which gets written to the serial format and indicates that the entry is a [TransactionCommandEntry].
         *
         * The `TYPE_BYTE` of all [WALEntry] implementations must be unique.
         *
         * Do not change this value, or all WAL files out there will become permanently invalid!
         */
        const val TYPE_BYTE: Byte = 0x20

        /** The integer representation of [TYPE_BYTE] for all APIs that need it. */
        const val TYPE_INT = TYPE_BYTE.toInt()


        /**
         * Reads a [TransactionCommandEntry] from the given [inputStream] while also validating its hash.
         *
         * This function assumes that the initial `TYPE_BYTE` has already been consumed to identify the type of entry to parse.
         *
         * @param inputStream The input stream to read from.
         *
         * @return The parsed [TransactionCommandEntry]
         *
         * @throws WriteAheadLogCorruptedException if the parsing process encountered an issue (including hash mismatch).
         */
        fun readFrom(inputStream: InputStream): TransactionCommandEntry {
            val savedHash: Int
            val entry: TransactionCommandEntry

            try {
                savedHash = inputStream.readLittleEndianInt()
                val storeId = StoreId.readFrom(inputStream)
                val command = Command.readFromStream(inputStream)
                entry = TransactionCommandEntry(storeId, command)
            } catch (e: Exception) {
                throw WriteAheadLogCorruptedException("Could not parse ${TransactionCommandEntry::class.simpleName} from WAL, WAL file is corrupted! ", e)
            }

            val currentHash = entry.hash(HASH_FUNCTION).asInt()
            if (savedHash != currentHash) {
                throw WriteAheadLogCorruptedException(
                    "Entry hash mismatch for ${TransactionCommandEntry::class.simpleName}, WAL file is corrupted!" +
                        " Hash according to input: ${savedHash}, hash after deserialization: ${currentHash}"
                )
            }

            return entry
        }

    }

    override val commitTSN: TSN
        get() = this.command.tsn

    override fun hash(hasher: Hasher): Hasher {
        return hasher
            .putByte(TYPE_BYTE)
            .putStoreId(this.storeId)
            .putCommand(this.command)
    }

    override fun writeTo(outputStream: OutputStream) {
        val hashFunction = WALEntry.HASH_FUNCTION
        val hash = this.hash(hashFunction).asInt()

        outputStream.write(TYPE_INT)
        outputStream.writeLittleEndianInt(hash)
        outputStream.write(this.storeId)
        outputStream.write(this.command)
    }

}