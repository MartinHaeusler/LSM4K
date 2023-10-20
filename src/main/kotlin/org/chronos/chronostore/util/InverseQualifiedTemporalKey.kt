package org.chronos.chronostore.util

import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.bits.BitTricks.readStableLong
import org.chronos.chronostore.util.bits.BitTricks.writeStableLong
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.bytes.Bytes.Companion.writeBytesWithoutSize
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
                // [timestamp bytes][storeName bytes][userKeyBytes]
                val timestamp = input.readStableLong()
                val storeId = StoreId.readFrom(input)
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
        this.storeId.writeTo(output)
        output.writeBytesWithoutSize(this.userKey)
        return Bytes.wrap(output.toByteArray())
    }

    override fun compareTo(other: InverseQualifiedTemporalKey): Int {
        val timestampCmp = this.timestamp.compareTo(other.timestamp)
        if (timestampCmp != 0) {
            return timestampCmp
        }
        val storeNameCmp = this.storeId.compareTo(other.storeId)
        if (storeNameCmp != 0) {
            return storeNameCmp
        }
        return this.userKey.compareTo(other.userKey)
    }

    override fun toString(): String {
        return "InverseQualifiedTemporalKey[${this.timestamp}->${this.storeId}.${this.userKey}]"
    }
}