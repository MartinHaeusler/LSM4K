package org.chronos.chronostore.wal

import org.chronos.chronostore.api.exceptions.TruncatedInputException
import org.chronos.chronostore.api.exceptions.WriteAheadLogEntryCorruptedException
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianLong
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianLong
import org.chronos.chronostore.util.PrefixIO
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.StoreId.Companion.write
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.UUIDExtensions.readUUIDFrom
import org.chronos.chronostore.util.UUIDExtensions.toBytes
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.bytes.Bytes.Companion.writeBytesWithoutSize
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.zip.CRC32
import java.util.zip.CheckedInputStream
import java.util.zip.CheckedOutputStream

/**
 * Utility class for reading/writing [WriteAheadLogTransaction]s from/to the Write Ahead Log file.
 *
 * The format looks as follows:
 *
 * ```
 * +--------------------------------------+--------+
 * | Transaction ID                       |        |
 * | Commit Timestamp                     | HEADER |
 * | Compression Algorithm                |        |
 * | Commit Metadata                      |        |
 * +--------------------------------------+--------+
 * | Block 1 Header                       |        |
 * |   Target Store ID                    |  BODY  |
 * +--------------------------------------+        |
 * |     Block 1 Body                     |        |
 * |       Key + Operation                |        |
 * |       Key + Operation                |  BODY  |
 * |       ...                            |        |
 * |       Key + Operation                |        |
 * +--------------------------------------+        |
 * |     Block 1 Footer                   |        |
 * |       Block Checksum                 |  BODY  |
 * +--------------------------------------+        |
 * | Block 2 Header                       |        |
 * |   Target Store ID                    |        |
 * +--------------------------------------+        |
 * |     Block 2 Body                     |        |
 * |       Key + Operation                |  BODY  |
 * |       Key + Operation                |        |
 * |       ...                            |        |
 * |       Key + Operation                |        |
 * +--------------------------------------+        |
 * |     Block 2 Footer                   |  BODY  |
 * |       Block Checksum                 |        |
 * +--------------------------------------+        |
 * |   ...                                |        |
 * +--------------------------------------+        |
 * | Block N Header                       |  BODY  |
 * |   Target Store ID                    |        |
 * +--------------------------------------+        |
 * |     Block N Body                     |        |
 * |       Key + Operation                |  BODY  |
 * |       Key + Operation                |        |
 * |       ...                            |        |
 * |       Key + Operation                |        |
 * +--------------------------------------+        |
 * |     Block N Footer                   |  BODY  |
 * |       Block Checksum                 |        |
 * +--------------------------------------+--------+
 * | Entry Checksum                       | FOOTER |
 * | Magic Byte                           |        |
 * +--------------------------------------+--------+
 * ```
 *
 * It's worth noting that:
 *
 * - The *entire* body is being compressed as one block.
 * - The compression algorithm is contained in the header.
 *   In theory, every block may have its own compression
 *   algorithm. This allows us to change the WAL compression
 *   algorithm over time.
 * - The StoreID of every store is only encoded once, and only
 *   if this store has received changes from the transaction.
 * - The footer contains a checksum for the **compressed**
 *   bytes. This allows us to check for corrupted or truncated
 *   entries. The checksum is non-cryptographic.
 * - The entire entry is terminated by a magic byte. This is
 *   a fixed sequence of 8 bytes which is used as a "checkpoint".
 *   If an entry is not terminated by these magic bytes, it is
 *   considered invalid. This is another safeguard against
 *   truncation.
 * - Each entry is written atomically to the WAL, i.e. no
 *   interleavings between entries are allowed. This means
 *   that this WAL format can only process one transaction
 *   commit at a time, which is fine because we expect the
 *   transaction commit timestamps to be unique.
 */
object WriteAheadLogFormat {

    val ENTRY_MAGIC_BYTES = Bytes.wrap(
        byteArrayOf(
            0b01100101, // e
            0b01101110, // n
            0b01110100, // t
            0b01110010, // r
            0b01111001, // y
            0b01100101, // e
            0b01101110, // n
            0b01100100, // d
        )               // entryend = end of entry
    )

    // =================================================================================================================
    // WRITER
    // =================================================================================================================

    fun writeTransaction(
        tx: WriteAheadLogTransaction,
        compressionAlgorithm: CompressionAlgorithm,
        out: OutputStream
    ) {
        writeHeader(tx, compressionAlgorithm, out)
        val checksum = writeBody(tx, compressionAlgorithm, out)
        writeFooter(checksum, out)
    }

    private fun writeHeader(
        tx: WriteAheadLogTransaction,
        compressionAlgorithm: CompressionAlgorithm,
        out: OutputStream,
    ) {
        PrefixIO.writeBytes(out, tx.transactionId.toBytes())
        out.writeLittleEndianLong(tx.commitTimestamp)
        out.writeLittleEndianInt(compressionAlgorithm.algorithmIndex)
        PrefixIO.writeBytes(out, tx.commitMetadata)
    }

    private fun writeBody(
        tx: WriteAheadLogTransaction,
        compressionAlgorithm: CompressionAlgorithm,
        out: OutputStream
    ): Long {
        val bodyBytes = ByteArrayOutputStream().use { baos ->
            // each changed store gives rise to one "block".
            for ((storeId, commands) in tx.storeIdToCommands) {
                val bodyChecksum = CheckedOutputStream(baos, CRC32()).use { blockOut ->
                    // write the block header
                    blockOut.write(storeId)
                    blockOut.writeLittleEndianInt(commands.size)
                    // write the block body
                    for (command in commands) {
                        command.writeToStream(blockOut)
                    }
                    blockOut.flush()
                    blockOut.checksum.value
                }
                // write the block footer
                baos.writeLittleEndianLong(bodyChecksum)
            }
            baos.toByteArray()
        }
        val compressedBody = if(bodyBytes.isEmpty()){
            Bytes.EMPTY
        }else{
            Bytes.wrap(compressionAlgorithm.compress(bodyBytes))
        }
        PrefixIO.writeBytes(out, compressedBody)
        return crc32(compressedBody)
    }

    private fun writeFooter(checksum: Long, out: OutputStream) {
        out.writeLittleEndianLong(checksum)
        out.writeBytesWithoutSize(ENTRY_MAGIC_BYTES)
    }


    // =================================================================================================================
    // READER
    // =================================================================================================================

    /**
     * Reads a [WriteAheadLogTransaction] from the given [input].
     *
     * @param input The input stream to read from.
     *
     * @return The transaction, or `null` if [input] has no more data to read.
     */
    fun readTransaction(
        input: InputStream
    ): WriteAheadLogTransaction? {
        val header = readHeader(input)
        // read the body
        val compressedBody = PrefixIO.readBytes(input)
        val expectedChecksum = input.readLittleEndianLong()
        val magicBytes = Bytes.wrap(input.readNBytes(ENTRY_MAGIC_BYTES.size))
        if(magicBytes.size < ENTRY_MAGIC_BYTES.size){
            throw TruncatedInputException("WAL entry has been truncated!")
        }
        checkForValidity(magicBytes == ENTRY_MAGIC_BYTES) {
            "Error reading WAL entry: The magic byte sequence at the end doesn't match (expected: ${magicBytes}, found: ${magicBytes})!" +
                " The entry is corrupted or truncated!"
        }
        val actualChecksum = crc32(compressedBody)
        checkForValidity(actualChecksum == expectedChecksum) {
            "Error reading WAL entry: The checksum doesn't match (expected: ${expectedChecksum}, found: ${actualChecksum})!" +
                " The entry is corrupted or truncated!"
        }
        // decompress and decode the body
        val decompressedBody = if (compressedBody.isEmpty()) {
            Bytes.EMPTY
        } else {
            header.compressionAlgorithm.decompress(compressedBody)
        }
        val storeIdToCommands = decompressedBody.withInputStream(this::decodeBody)
        return WriteAheadLogTransaction(
            transactionId = header.transactionId,
            commitTimestamp = header.commitTimestamp,
            storeIdToCommands = storeIdToCommands,
            commitMetadata = header.commitMetadata,
        )
    }

    private fun readHeader(input: InputStream): Header {
        val transactionIdBytes = PrefixIO.readBytes(input)
        val transactionId = readUUIDFrom(transactionIdBytes)
        val commitTimestamp = input.readLittleEndianLong()
        checkForValidity(commitTimestamp >= 0) {
            "Error reading WAL entry header: commit timestamp must not be negative (${commitTimestamp})!" +
                " The entry is corrupted or truncated!"
        }
        val compressionAlgorithm = CompressionAlgorithm.fromAlgorithmIndex(input.readLittleEndianInt())
        val commitMetadata = PrefixIO.readBytes(input)
        return Header(
            transactionId = transactionId,
            commitTimestamp = commitTimestamp,
            commitMetadata = commitMetadata,
            compressionAlgorithm = compressionAlgorithm,
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun decodeBody(inputStream: InputStream): Map<StoreId, List<Command>> {
        val storeIdToCommands = mutableMapOf<StoreId, List<Command>>()
        while (true) {
            var storeId: StoreId?
            var commands: List<Command>? = null
            val actualChecksum = CheckedInputStream(inputStream, CRC32()).use { checkedIn ->
                storeId = StoreId.readFromOrNull(checkedIn)
                if (storeId == null) {
                    // input stream has no more data
                    return@use -1L
                }
                val commandCount = checkedIn.readLittleEndianInt()
                val commandsArray = arrayOfNulls<Command>(commandCount)
                repeat(commandCount) { i ->
                    commandsArray[i] = Command.readFromStream(checkedIn)
                }
                // we've overwritten all NULL values in the array, so the following cast is safe.
                commands = commandsArray.asList() as List<Command>

                return@use checkedIn.checksum.value
            }
            if (storeId == null) {
                // no more entries in the input
                break
            }
            val expectedChecksum = inputStream.readLittleEndianLong()
            checkForValidity(actualChecksum == expectedChecksum) {
                "Error reading WAL body entry for store '${storeId}': The checksum doesn't match (expected: ${expectedChecksum}, found: ${actualChecksum})!" +
                    " The entry is corrupted or truncated!"
            }
            storeIdToCommands[storeId!!] = commands!!
        }
        return storeIdToCommands
    }

    private fun crc32(byteArray: ByteArray): Long {
        val checksum = CRC32()
        checksum.update(byteArray)
        return checksum.value
    }

    private fun crc32(bytes: Bytes): Long {
        val checksum = CRC32()
        checksum.update(bytes.toSharedArray())
        return checksum.value
    }

    private inline fun checkForValidity(condition: Boolean, msg: () -> String) {
        if (!condition) {
            throw WriteAheadLogEntryCorruptedException(msg())
        }
    }

    private class Header(
        val transactionId: TransactionId,
        val commitTimestamp: Timestamp,
        val commitMetadata: Bytes,
        val compressionAlgorithm: CompressionAlgorithm,
    )
}