package org.chronos.chronostore.wal

import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.Bytes.Companion.write
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianLong
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianLong
import org.chronos.chronostore.util.PrefixIO
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.UUIDExtensions.readUUIDFrom
import org.chronos.chronostore.util.UUIDExtensions.toBytes
import java.io.InputStream
import java.io.OutputStream
import java.util.*

sealed interface WriteAheadLogEntry {

    companion object {

        fun beginTransaction(transactionId: UUID): BeginTransactionEntry {
            return BeginTransactionEntry(transactionId)
        }

        fun put(transactionId: UUID, key: Bytes, value: Bytes): PutEntry {
            return PutEntry(transactionId, key, value)
        }

        fun delete(transactionId: UUID, key: Bytes): DeleteEntry {
            return DeleteEntry(transactionId, key)
        }

        fun commit(transactionId: UUID, commitTimestamp: Timestamp): CommitTransactionEntry {
            return CommitTransactionEntry(transactionId, commitTimestamp)
        }

        fun createWriteAheadLogForTransaction(
            transactionId: UUID,
            puts: Map<Bytes, Bytes>,
            deletes: Set<Bytes>,
            commitTimestamp: Timestamp,
        ): List<WriteAheadLogEntry> {
            val list = mutableListOf<WriteAheadLogEntry>()
            list.add(beginTransaction(transactionId))
            for ((key, value) in puts) {
                list.add(put(transactionId, key, value))
            }
            for (key in deletes) {
                list.add(delete(transactionId, key))
            }
            list.add(commit(transactionId, commitTimestamp))
            return list
        }

        fun readSingleEntryFrom(inputStream: InputStream): WriteAheadLogEntry {
            return when (val typeByte = inputStream.read()) {
                BeginTransactionEntry.TYPE_BYTE -> {
                    BeginTransactionEntry.readFromWithoutTypeByte(inputStream)
                }

                PutEntry.TYPE_BYTE -> {
                    PutEntry.readFromWithoutTypeByte(inputStream)
                }

                DeleteEntry.TYPE_BYTE -> {
                    DeleteEntry.readFromWithoutTypeByte(inputStream)
                }

                CommitTransactionEntry.TYPE_BYTE -> {
                    CommitTransactionEntry.readFromWithoutTypeByte(inputStream)
                }

                else -> {
                    throw IllegalStateException("Cannot read WAL entry from typeByte ${typeByte}!")
                }
            }
        }

    }

    val transactionId: UUID

    fun writeTo(outputStream: OutputStream)


    data class BeginTransactionEntry(
        override val transactionId: UUID,
    ) : WriteAheadLogEntry {

        companion object {

            const val TYPE_BYTE = 100

            fun readFromWithoutTypeByte(inputStream: InputStream): BeginTransactionEntry {
                val transactionId = readUUIDFrom(inputStream.readNBytes(Long.SIZE_BYTES * 2))
                return BeginTransactionEntry(transactionId)
            }

        }

        override fun writeTo(outputStream: OutputStream) {
            outputStream.write(TYPE_BYTE)
            outputStream.write(this.transactionId.toBytes())
        }

    }

    data class PutEntry(
        override val transactionId: UUID,
        val key: Bytes,
        val value: Bytes,
    ) : WriteAheadLogEntry {

        companion object {

            const val TYPE_BYTE = 10

            fun readFromWithoutTypeByte(inputStream: InputStream): PutEntry {
                val transactionId = readUUIDFrom(inputStream.readNBytes(Long.SIZE_BYTES * 2))
                val key = PrefixIO.readBytes(inputStream)
                val value = PrefixIO.readBytes(inputStream)
                return PutEntry(transactionId, key, value)
            }

        }

        override fun writeTo(outputStream: OutputStream) {
            outputStream.write(TYPE_BYTE)
            outputStream.write(transactionId.toBytes())
            PrefixIO.writeBytes(outputStream, this.key)
            PrefixIO.writeBytes(outputStream, this.value)
        }

    }

    data class DeleteEntry(
        override val transactionId: UUID,
        val key: Bytes,
    ) : WriteAheadLogEntry {

        companion object {

            const val TYPE_BYTE = 20

            fun readFromWithoutTypeByte(inputStream: InputStream): DeleteEntry {
                val transactionId = readUUIDFrom(inputStream.readNBytes(Long.SIZE_BYTES * 2))
                val key = PrefixIO.readBytes(inputStream)
                return DeleteEntry(transactionId, key)
            }

        }

        override fun writeTo(outputStream: OutputStream) {
            outputStream.write(TYPE_BYTE)
            outputStream.write(transactionId.toBytes())
            PrefixIO.writeBytes(outputStream, this.key)
        }

    }

    data class CommitTransactionEntry(
        override val transactionId: UUID,
        val commitTimestamp: Timestamp,
    ) : WriteAheadLogEntry {

        companion object {

            const val TYPE_BYTE = 110

            fun readFromWithoutTypeByte(inputStream: InputStream): CommitTransactionEntry {
                val transactionId = readUUIDFrom(inputStream.readNBytes(Long.SIZE_BYTES * 2))
                val commitTimestamp = inputStream.readLittleEndianLong()
                return CommitTransactionEntry(transactionId, commitTimestamp)
            }

        }

        init {
            require(commitTimestamp >= 0) {
                "Argument 'commitTimestamp' (${commitTimestamp}) must not be negative!"
            }
        }


        override fun writeTo(outputStream: OutputStream) {
            outputStream.write(TYPE_BYTE)
            outputStream.write(transactionId.toBytes())
            outputStream.writeLittleEndianLong(commitTimestamp)
        }

    }

}


