package io.github.martinhaeusler.lsm4k.wal.format

import com.google.common.hash.Hasher
import io.github.martinhaeusler.lsm4k.api.exceptions.WriteAheadLogCorruptedException
import io.github.martinhaeusler.lsm4k.impl.annotations.PersistentClass
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.readLittleEndianInt
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.readLittleEndianLong
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.writeLittleEndianInt
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.writeLittleEndianLong
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.wal.format.TransactionCommitEntry.Companion.TYPE_BYTE
import io.github.martinhaeusler.lsm4k.wal.format.WALEntry.Companion.HASH_FUNCTION
import java.io.InputStream
import java.io.OutputStream

/**
 * A [WALEntry] which signifies the commit of a transaction.
 *
 * Every individual [TransactionCommitEntry] adheres to the following binary schema when serialized
 * (excluding the type byte and hash which is common to all [WALEntry] classes):
 *
 * ```
 *       5 Byte         8 Byte
 * +-----------------+------------+
 * | WALEntry header | commit TSN |
 * +-----------------+------------+
 * ```
 *
 * - `WALEntry header` is the common header of all entries described in [WALEntry].
 * - `commit TSN` is a little-endian long which represents the [commitTSN].
 */
@PersistentClass(format = PersistentClass.Format.BINARY, details = "Used in WAL.")
data class TransactionCommitEntry(
    override val commitTSN: TSN,
) : WALEntry {

    companion object {

        /**
         * A [Byte] which gets written to the serial format and indicates that the entry is a [TransactionCommitEntry].
         *
         * The `TYPE_BYTE` of all [WALEntry] implementations must be unique.
         *
         * Do not change this value, or all WAL files out there will become permanently invalid!
         */
        const val TYPE_BYTE: Byte = 0x30

        /** The integer representation of [TYPE_BYTE] for all APIs that need it. */
        const val TYPE_INT = TYPE_BYTE.toInt()

        /**
         * Reads a [TransactionCommitEntry] from the given [inputStream] while also validating its hash.
         *
         * This function assumes that the initial `TYPE_BYTE` has already been consumed to identify the type of entry to parse.
         *
         * @param inputStream The input stream to read from.
         *
         * @return The parsed [TransactionCommitEntry]
         *
         * @throws WriteAheadLogCorruptedException if the parsing process encountered an issue (including hash mismatch).
         */
        fun readFrom(inputStream: InputStream): TransactionCommitEntry {
            val savedHash: Int
            val entry: TransactionCommitEntry

            try {
                savedHash = inputStream.readLittleEndianInt()
                val commitTsn = inputStream.readLittleEndianLong()
                entry = TransactionCommitEntry(commitTsn)
            } catch (e: Exception) {
                throw WriteAheadLogCorruptedException("Could not parse ${TransactionCommitEntry::class.simpleName} from WAL, WAL file is corrupted!", e)
            }

            val currentHash = entry.hash(HASH_FUNCTION).asInt()
            if (currentHash != savedHash) {
                throw WriteAheadLogCorruptedException(
                    "Entry hash mismatch for ${TransactionCommitEntry::class.simpleName}, WAL file is corrupted!" +
                        " Hash according to input: ${savedHash}, hash after deserialization: ${currentHash}"
                )
            }

            return entry
        }

    }

    override fun hash(hasher: Hasher): Hasher {
        return hasher
            .putByte(TYPE_BYTE)
            .putLong(this.commitTSN)
    }

    override fun writeTo(outputStream: OutputStream) {
        val hashFunction = WALEntry.HASH_FUNCTION
        val hash = this.hash(hashFunction).asInt()

        outputStream.write(TYPE_INT)
        outputStream.writeLittleEndianInt(hash)
        outputStream.writeLittleEndianLong(this.commitTSN)
    }

}