package org.chronos.chronostore.io.format

import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.Bytes.Companion.write
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianLong
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianInt
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianLong
import org.chronos.chronostore.util.PrefixIO
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.UUIDExtensions.readUUIDFrom
import org.chronos.chronostore.util.UUIDExtensions.toBytes
import org.chronos.chronostore.util.unit.Bytes
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.UUID

class FileMetaData(
    /** The settings used to write the file. */
    val settings: ChronoStoreFileSettings,
    /** The UUID of the file. */
    val fileUUID: UUID,
    /** The smallest key in the file (independent of the timestamp). May be `null` if the file is empty. */
    val minKey: Bytes?,
    /** The largest key in the file (independent of the timestamp). May be `null` if the file is empty. */
    val maxKey: Bytes?,
    /** The smallest timestamp in the file (independent of the key). May be `null` if the file is empty. */
    val minTimestamp: Timestamp?,
    /** The largest timestamp in the file (independent of the key). May be `null` if the file is empty. */
    val maxTimestamp: Timestamp?,
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
    /** A map of additional, arbitrary key-value pairs. */
    val infoMap: Map<Bytes, Bytes>
) {

    companion object {

        @JvmStatic
        fun readFrom(inputStream: InputStream): FileMetaData {
            val settings = ChronoStoreFileSettings(
                compression = CompressionAlgorithm.fromAlgorithmIndex(inputStream.readLittleEndianInt()),
                maxBlockSize = inputStream.readLittleEndianInt().Bytes,
                indexRate = inputStream.readLittleEndianInt(),
            )
            val fileUUID = readUUIDFrom(inputStream.readNBytes(Long.SIZE_BYTES * 2))
            val minKey = PrefixIO.readBytes(inputStream).takeIf { it.isNotEmpty() }
            val maxKey = PrefixIO.readBytes(inputStream).takeIf { it.isNotEmpty() }
            val minTimestamp = inputStream.readLittleEndianLong().takeIf { it > 0 }
            val maxTimestamp = inputStream.readLittleEndianLong().takeIf { it > 0 }
            val headEntries = inputStream.readLittleEndianLong()
            val totalEntries = inputStream.readLittleEndianLong()
            val numberOfBlocks = inputStream.readLittleEndianInt()
            val numberOfMerges = inputStream.readLittleEndianLong()
            val createdAt = inputStream.readLittleEndianLong()
            val infoMapSize = inputStream.readLittleEndianInt()
            val mapInput = ByteArrayInputStream(inputStream.readNBytes(infoMapSize))
            val infoMap = mutableMapOf<Bytes, Bytes>()
            while (true) {
                val key = PrefixIO.readBytesOrNull(mapInput)
                    ?: break
                val value = PrefixIO.readBytes(mapInput)
                infoMap[key] = value
            }
            return FileMetaData(
                settings = settings,
                fileUUID = fileUUID,
                minKey = minKey,
                maxKey = maxKey,
                minTimestamp = minTimestamp,
                maxTimestamp = maxTimestamp,
                headEntries = headEntries,
                totalEntries = totalEntries,
                numberOfBlocks = numberOfBlocks,
                numberOfMerges = numberOfMerges,
                createdAt = createdAt,
                infoMap = infoMap,
            )
        }

    }

    fun writeTo(outputStream: OutputStream) {
        outputStream.writeLittleEndianInt(this.settings.compression.algorithmIndex)
        outputStream.writeLittleEndianInt(this.settings.maxBlockSize.bytes.toInt())
        outputStream.writeLittleEndianInt(this.settings.indexRate)
        outputStream.write(fileUUID.toBytes())
        PrefixIO.writeBytes(outputStream, this.minKey ?: Bytes.EMPTY)
        PrefixIO.writeBytes(outputStream, this.maxKey ?: Bytes.EMPTY)
        outputStream.writeLittleEndianLong(this.minTimestamp ?: 0)
        outputStream.writeLittleEndianLong(this.maxTimestamp ?: 0)
        outputStream.writeLittleEndianLong(this.headEntries)
        outputStream.writeLittleEndianLong(this.totalEntries)
        outputStream.writeLittleEndianInt(this.numberOfBlocks)
        outputStream.writeLittleEndianLong(this.numberOfMerges)
        outputStream.writeLittleEndianLong(this.createdAt)
        val infoMapSize = this.infoMap.entries.sumOf { it.key.size + Int.SIZE_BYTES + it.value.size + Int.SIZE_BYTES }
        outputStream.writeLittleEndianInt(infoMapSize)
        for ((key, value) in infoMap) {
            PrefixIO.writeBytes(outputStream, key)
            PrefixIO.writeBytes(outputStream, value)
        }
    }

    fun mayContainKey(key: Bytes): Boolean {
        if (this.minKey == null || this.maxKey == null) {
            // this file is empty -> it contains NO keys at all
            return false
        }
        // check if the key is in range
        return this.minKey <= key && key <= this.maxKey
    }

    fun mayContainDataRelevantForTimestamp(timestamp: Timestamp): Boolean {
        if (this.minTimestamp == null) {
            // the file is empty
            return false
        }
        // if the min timestamp is GREATER than the request timestamp,
        // this file only contains data which is NEWER than the timestamp
        // we're looking for, thus there's no data in this file affecting that timestamp.
        return this.minTimestamp <= timestamp
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

}