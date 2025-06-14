package io.github.martinhaeusler.lsm4k.io.format

import io.github.martinhaeusler.lsm4k.model.command.KeyAndTSN
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.readLittleEndianInt
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.readLittleEndianLong
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.writeLittleEndianInt
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.writeLittleEndianLong
import io.github.martinhaeusler.lsm4k.util.PrefixIO
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.util.Timestamp
import io.github.martinhaeusler.lsm4k.util.UUIDExtensions.readUUIDFrom
import io.github.martinhaeusler.lsm4k.util.UUIDExtensions.toBytes
import io.github.martinhaeusler.lsm4k.util.bloom.BytesBloomFilter
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes.Companion.writeBytesWithoutSize
import java.io.InputStream
import java.io.OutputStream
import java.util.*

class FileMetaData(
    /** The settings used to write the file. */
    val settings: LSMFileSettings,
    /** The UUID of the file. */
    val fileUUID: UUID,
    /** The smallest key in the file (independent of the TSN). May be `null` if the file is empty. */
    val minKey: Bytes?,
    /** The largest key in the file (independent of the TSN). May be `null` if the file is empty. */
    val maxKey: Bytes?,
    /** The first key in the file (including the TSN). May be `null` if the file is empty. */
    val firstKeyAndTSN: KeyAndTSN?,
    /** The last key in the file (including the TSN). May be `null` if the file is empty. */
    val lastKeyAndTSN: KeyAndTSN?,

    /** The smallest [TSN] in the file (independent of the key). May be `null` if the file is empty. */
    val minTSN: TSN?,
    /**
     * The largest [TSN] in the file (independent of the key).
     *
     * May be `null` if the file is empty.
     *
     * Please note that just because a file contains data from a given TSN, it does **not** mean that
     * the corresponding transaction has been **fully** reflected in this file (or any other LSM file).
     * Some parts of the data may still be missing. To see the maximum TSN which has been fully represented
     * by this file (including previous LSM files), please use [maxCompletelyWrittenTSN]
     */
    val maxTSN: TSN?,
    /**
     * The highest [TSN] which is guaranteed to be completely contained in this file (or previous already written LSM files).
     *
     * May be `null` if no transaction has been fully completed.
     */
    val maxCompletelyWrittenTSN: TSN?,
    /** The number of non-overwritten non-delete entries in this file (i.e. the size of the "head" key set of the file). */
    val headEntries: Long,
    /** The total number of entries in this file. */
    val totalEntries: Long,
    /** The total number of data blocks in this file. May be zero if the file is empty. */
    val numberOfBlocks: Int,
    /** How often a merge operation has been applied to produce this file (on merge, computed via `1 + max(numberOfMerges)`). Starts at 0.*/
    val numberOfMerges: Long,
    /** The wall-clock-timestamp this file was written at (begin of write). */
    val createdAt: Timestamp,
    /** The bloom filter for the file; indicates which keys *may* appear in the file. */
    val bloomFilter: BytesBloomFilter,
) {

    companion object {

        @JvmStatic
        fun readFrom(inputStream: InputStream): FileMetaData {
            val settings = LSMFileSettings.readFrom(inputStream)
            val fileUUID = readUUIDFrom(inputStream.readNBytes(Long.SIZE_BYTES * 2))
            val minKey = PrefixIO.readBytes(inputStream).takeIf { it.isNotEmpty() }
            val maxKey = PrefixIO.readBytes(inputStream).takeIf { it.isNotEmpty() }
            val firstKeyAndTSN = PrefixIO.readBytes(inputStream).takeIf { it.isNotEmpty() }?.let(KeyAndTSN::readFromBytes)
            val lastKeyAndTSN = PrefixIO.readBytes(inputStream).takeIf { it.isNotEmpty() }?.let(KeyAndTSN::readFromBytes)
            val minTSN = inputStream.readLittleEndianLong().takeIf { it > 0 }
            val maxTSN = inputStream.readLittleEndianLong().takeIf { it > 0 }
            val maxCompletelyWrittenTSN = inputStream.readLittleEndianLong().takeIf { it > 0 }
            val headEntries = inputStream.readLittleEndianLong()
            val totalEntries = inputStream.readLittleEndianLong()
            val numberOfBlocks = inputStream.readLittleEndianInt()
            val numberOfMerges = inputStream.readLittleEndianLong()
            val createdAt = inputStream.readLittleEndianLong()
            val bloomFilterSize = inputStream.readLittleEndianInt()
            val bloomFilterBytes = BytesBloomFilter.readFrom(Bytes.wrap(inputStream.readNBytes(bloomFilterSize)))
            return FileMetaData(
                settings = settings,
                fileUUID = fileUUID,
                minKey = minKey,
                maxKey = maxKey,
                firstKeyAndTSN = firstKeyAndTSN,
                lastKeyAndTSN = lastKeyAndTSN,
                minTSN = minTSN,
                maxTSN = maxTSN,
                maxCompletelyWrittenTSN = maxCompletelyWrittenTSN,
                headEntries = headEntries,
                totalEntries = totalEntries,
                numberOfBlocks = numberOfBlocks,
                numberOfMerges = numberOfMerges,
                createdAt = createdAt,
                bloomFilter = bloomFilterBytes,
            )
        }

    }

    fun writeTo(outputStream: OutputStream) {
        this.settings.writeTo(outputStream)
        outputStream.writeBytesWithoutSize(fileUUID.toBytes())
        PrefixIO.writeBytes(outputStream, this.minKey ?: Bytes.EMPTY)
        PrefixIO.writeBytes(outputStream, this.maxKey ?: Bytes.EMPTY)
        PrefixIO.writeBytes(outputStream, this.firstKeyAndTSN?.toBytes() ?: Bytes.EMPTY)
        PrefixIO.writeBytes(outputStream, this.lastKeyAndTSN?.toBytes() ?: Bytes.EMPTY)
        outputStream.writeLittleEndianLong(this.minTSN ?: 0)
        outputStream.writeLittleEndianLong(this.maxTSN ?: 0)
        outputStream.writeLittleEndianLong(this.maxCompletelyWrittenTSN ?: 0)
        outputStream.writeLittleEndianLong(this.headEntries)
        outputStream.writeLittleEndianLong(this.totalEntries)
        outputStream.writeLittleEndianInt(this.numberOfBlocks)
        outputStream.writeLittleEndianLong(this.numberOfMerges)
        outputStream.writeLittleEndianLong(this.createdAt)
        val bloomFilterBytes = this.bloomFilter.toBytes()
        outputStream.writeLittleEndianInt(bloomFilterBytes.size)
        outputStream.writeBytesWithoutSize(bloomFilterBytes)
    }

    fun mayContainKey(key: Bytes): Boolean {
        if (this.minKey == null || this.maxKey == null) {
            // this file is empty -> it contains NO keys at all
            return false
        }
        // check if the key is in range
        if (key < this.minKey || key > this.maxKey) {
            // key is out of range
            return false
        }
        return this.bloomFilter.mightContain(key)
    }

    fun overlaps(minKey: Bytes, maxKey: Bytes): Boolean {
        if (this.minKey == null || this.maxKey == null) {
            return false
        }
        if (maxKey < this.minKey) {
            // sst range starts after the search range
            return false
        }
        if (minKey > this.maxKey) {
            // sst range ends before the search range
            return false
        }
        return true
    }

    fun mayContainDataRelevantForTSN(tsn: TSN): Boolean {
        if (this.minTSN == null) {
            // the file is empty
            return false
        }
        // if the min TSN is GREATER than the request TSN,
        // this file only contains data which is NEWER than the TSN
        // we're looking for, thus there's no data in this file affecting that TSN.
        return this.minTSN <= tsn
    }

    /** The number of entries in this file which have been overwritten by newer entries on the same keys. */
    val historyEntries: Long
        get() = totalEntries - headEntries

    val headHistoryRatio: Double
        get() {
            if (this.totalEntries <= 0) {
                return 1.0 // by definition
            }
            return this.headEntries.toDouble() / this.totalEntries
        }

    val sizeBytes: Long
        get() {
            val settingsSize = this.settings.sizeBytes
            // UUID = 128 bits = 2 x 64 bits
            val fileUUIDSize = Long.SIZE_BYTES * 2
            val minKeySize = Int.SIZE_BYTES + (this.minKey?.size ?: 0)
            val maxKeySize = Int.SIZE_BYTES + (this.maxKey?.size ?: 0)
            val firstKeyAndTSNSize = Int.SIZE_BYTES + (this.firstKeyAndTSN?.byteSize ?: 0)
            val lastKeyAndTSNSize = Int.SIZE_BYTES + (this.lastKeyAndTSN?.byteSize ?: 0)
            val minTSNSize = TSN.SIZE_BYTES
            val maxTSNSize = TSN.SIZE_BYTES
            val maxCompletelyWrittenTSNSize = TSN.SIZE_BYTES
            val statsSize = Long.SIZE_BYTES * 3 + Int.SIZE_BYTES
            val createdAtSize = Timestamp.SIZE_BYTES
            val bloomFilterSize = Int.SIZE_BYTES + this.bloomFilter.sizeBytes
            return settingsSize +
                fileUUIDSize +
                minKeySize +
                maxKeySize +
                firstKeyAndTSNSize +
                lastKeyAndTSNSize +
                minTSNSize +
                maxTSNSize +
                maxCompletelyWrittenTSNSize +
                statsSize +
                createdAtSize +
                bloomFilterSize
        }

    override fun toString(): String {
        return "FileMetaData(settings=$settings, fileUUID=$fileUUID, minKey=$minKey, maxKey=$maxKey, minTSN=$minTSN, maxTSN=$maxTSN, maxCompletelyWrittenTSN=$maxCompletelyWrittenTSN, headEntries=$headEntries, totalEntries=$totalEntries, numberOfBlocks=$numberOfBlocks, numberOfMerges=$numberOfMerges, createdAt=$createdAt, historyEntries=$historyEntries, headHistoryRatio=$headHistoryRatio)"
    }


}