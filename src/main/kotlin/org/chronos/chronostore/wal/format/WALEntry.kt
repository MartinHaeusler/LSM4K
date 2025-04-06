package org.chronos.chronostore.wal.format

import com.google.common.hash.HashFunction
import com.google.common.hash.Hashing
import org.chronos.chronostore.api.exceptions.TruncatedInputException
import org.chronos.chronostore.api.exceptions.WriteAheadLogCorruptedException
import org.chronos.chronostore.impl.annotations.PersistentClass
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.hash.Hashable
import org.chronos.chronostore.wal.format.WALEntry.Companion.readStreaming
import org.chronos.chronostore.wal.format.WALEntry.Companion.writeStreaming
import java.io.InputStream
import java.io.OutputStream

/**
 * A [WALEntry] represents a single entry in the Write-Ahead-Log (WAL).
 *
 * The WAL consists of a series of entries. Each entry contains its own checksum in the serialized format
 * in order to detect data corruption in the WAL files.
 *
 * The entry types are:
 *
 * - [TransactionStartEntry] signifies the beginning of a transaction.
 * - [TransactionCommandEntry] signifies a single change to a key-value pair in a single store.
 * - [TransactionCommitEntry] signifies the commit of a transaction.
 *
 * WAL entries can be [written][writeStreaming] to an [OutputStream] in a streaming fashion using a [Sequence].
 * WAL entries can also be [read][readStreaming] from an [InputStream] in a streaming fashion, producing a lazy [Sequence] of entries.
 *
 * Every individual [WALEntry] adheres to the following binary schema when serialized:
 *
 * ```
 *     1 Byte            4 Byte              variable length
 * +-------------+---------------------+-----------------------------+
 * |  TYPE BYTE  | MURMUR3 32-bit hash | Entry-Type-Specific Content |
 * +-------------+---------------------+-----------------------------+
 * ```
 *
 * - `TYPE BYTE` indicates which type of entry we're dealing with. Each [WALEntry] implementation defines a unique type byte.
 * - `MURMUR3 32-bit hash` contains a hash of the entry-specific content (and including the type byte) for validation.
 * - `Entry-Type-Specific Content` is of variable byte length and depends on the type of entry.
 *
 */
@PersistentClass(format = PersistentClass.Format.BINARY, details = "Used in WAL.")
sealed interface WALEntry: Hashable {

    /** The [TSN] of the commit to which this entry belongs. */
    val commitTSN: TSN

    companion object {

        /**
         * The hash function used on each particular entry of the WAL.
         *
         * ATTENTION: DO NOT CHANGE THIS! Changing this will make all WAL files out there permanently INVALID!
         */
        val HASH_FUNCTION: HashFunction = Hashing.murmur3_32_fixed()

        /**
         * Reads a single [WALEntry] from the given [inputStream].
         *
         * @param inputStream The input stream to read the data from.
         * @param ignoreTruncatedEntries Use `true` if truncated entries at the end of the stream
         *                               should result in a return value of `null`. Use `false` if a
         *                               truncated entry should result in an exception.
         *
         * @return The deserialized entry, or `null` if the given [inputStream] contains no data.
         *
         * @throws WriteAheadLogCorruptedException if the data in the input stream could not be parsed.
         */
        fun readFrom(inputStream: InputStream, ignoreTruncatedEntries: Boolean = false): WALEntry? {
            val typeByte = inputStream.read()
            if (typeByte < 0) {
                // end of input
                return null
            }
            return try {
                when (typeByte) {
                    TransactionStartEntry.TYPE_INT -> TransactionStartEntry.readFrom(inputStream)
                    TransactionCommandEntry.TYPE_INT -> TransactionCommandEntry.readFrom(inputStream)
                    TransactionCommitEntry.TYPE_INT -> TransactionCommitEntry.readFrom(inputStream)
                    else -> throw WriteAheadLogCorruptedException("Could not read ${WALEntry::class.simpleName} from input: unknown entry type ${typeByte}!")
                }
            } catch (e: WriteAheadLogCorruptedException) {
                if (e.cause is TruncatedInputException && ignoreTruncatedEntries) {
                    return null
                } else {
                    throw e
                }
            } catch (e: TruncatedInputException) {
                if (ignoreTruncatedEntries) {
                    return null
                } else {
                    throw e
                }
            }
        }

        /**
         * Reads [WALEntries][WALEntry] from the given [inputStream] into a lazy [Sequence].
         *
         * If a single entry cannot be read or has a hash mismatch, a [WriteAheadLogCorruptedException] is thrown.
         *
         * If the end of the [inputStream] is reached, the sequence stops.
         * If the [inputStream] contains no data, an empty sequence is returned.
         *
         * @param inputStream the input stream to read.
         * @param ignoreTruncatedEntries Use `true` if truncated entries at the end of the stream
         *                               should be silently ignored. Use `false` if a truncated entry should result in an exception.
         *
         * @return a lazy sequence of WAL entries.
         */
        fun readStreaming(inputStream: InputStream, ignoreTruncatedEntries: Boolean = false): Sequence<WALEntry> {
            return generateSequence { this.readFrom(inputStream, ignoreTruncatedEntries) }
        }

        /**
         * Writes the [WALEntries][WALEntry] from the given [entries] sequence into the given [outputStream].
         *
         * The sequence is consumed in a lazy fashion and all data is immediately handed into the output stream
         * without any barriers.
         *
         * @param entries The entries to write. Will be fully iterated exactly once.
         * @param outputStream The output stream to receive the data.
         */
        fun writeStreaming(entries: Sequence<WALEntry>, outputStream: OutputStream) {
            for (entry in entries) {
                entry.writeTo(outputStream)
            }
        }

    }

    /**
     * Writes this entry to the given [outputStream].
     *
     * Please use [WALEntry.readFrom] to read it again.
     *
     * @param outputStream The output stream which should receive the data.
     *
     * @see WALEntry.writeStreaming
     * @see WALEntry.readStreaming
     */
    fun writeTo(outputStream: OutputStream)


}