package org.chronos.chronostore.util

import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.UUIDExtensions.readUUIDFrom
import org.chronos.chronostore.util.UUIDExtensions.toBytes
import org.chronos.chronostore.util.bits.BitTricks.readStableLong
import org.chronos.chronostore.util.bits.BitTricks.writeStableLong
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.bytes.Bytes.Companion.write
import java.io.ByteArrayOutputStream

data class InverseQualifiedTemporalKey(
    val timestamp: Timestamp,
    val storeId: StoreId,
    val userKey: Bytes,
) : Comparable<InverseQualifiedTemporalKey> {

    companion object {

        fun fromBytes(bytes: Bytes): InverseQualifiedTemporalKey {
            bytes.withInputStream { input ->
                // format:
                // [timestamp bytes][storeId bytes][userKeyBytes]
                val timestamp = input.readStableLong()
                val storeIdBytes = input.readNBytes(16)
                require(storeIdBytes.size == 16) { "Failed to read 16 bytes as StoreID from byte input!" }
                val storeId = readUUIDFrom(storeIdBytes)
                val userKey = Bytes.wrap(input.readAllBytes())
                return InverseQualifiedTemporalKey(timestamp, storeId, userKey)
            }
        }

    }


    fun toBytes(): Bytes {
        // format:
        // [timestamp bytes][storeId bytes][userKeyBytes]
        val output = ByteArrayOutputStream(Timestamp.SIZE_BYTES + 16 + userKey.size)
        output.writeStableLong(this.timestamp)
        output.write(this.storeId.toBytes())
        output.write(this.userKey)
        return Bytes.wrap(output.toByteArray())
    }

    override fun compareTo(other: InverseQualifiedTemporalKey): Int {
        val timestampCmp = this.timestamp.compareTo(other.timestamp)
        if (timestampCmp != 0) {
            return timestampCmp
        }
        val storeIdCmp = this.storeId.compareTo(other.storeId)
        if (storeIdCmp != 0) {
            return storeIdCmp
        }
        return this.userKey.compareTo(other.userKey)
    }

    override fun toString(): String {
        return "InverseQualifiedTemporalKey[${this.timestamp}->${this.storeId}.${this.userKey}]"
    }
}