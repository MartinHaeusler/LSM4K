package org.chronos.chronostore.wal

import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.*
import org.chronos.chronostore.util.Bytes.Companion.write
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianLong
import org.chronos.chronostore.util.LittleEndianExtensions.writeLittleEndianLong
import org.chronos.chronostore.util.UUIDExtensions.readUUIDFrom
import org.chronos.chronostore.util.UUIDExtensions.toBytes
import java.io.InputStream
import java.io.OutputStream
import java.util.*

sealed interface WriteAheadLogEntry {

    companion object {

        fun beginTransaction(transactionId: TransactionId): BeginTransactionEntry {
            return BeginTransactionEntry(transactionId)
        }

        fun put(transactionId: TransactionId, storeId: StoreId, key: Bytes, value: Bytes): PutEntry {
            return PutEntry(transactionId, storeId, key, value)
        }

        fun delete(transactionId: TransactionId, storeId: StoreId, key: Bytes): DeleteEntry {
            return DeleteEntry(transactionId, storeId, key)
        }

        fun commit(transactionId: TransactionId, commitTimestamp: Timestamp, commitMetadata: Bytes): CommitTransactionEntry {
            return CommitTransactionEntry(transactionId, commitTimestamp, commitMetadata)
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

        fun readTransactionActionsFrom(inputStream: InputStream): List<WriteAheadLogEntry> {
            val transactionActions = mutableListOf<WriteAheadLogEntry>()
            val firstAction = readSingleEntryFrom(inputStream)
            if (firstAction !is BeginTransactionEntry) {
                throw IllegalStateException(
                    "The first WAL entry read from the input stream is not" +
                        " a BeginTransactionEntry (it is of type '${firstAction::class.simpleName}')!"
                )
            }
            transactionActions.add(firstAction)
            val transactionId = firstAction.transactionId
            while (true) {
                val entry = readSingleEntryFrom(inputStream)
                if (entry.transactionId != transactionId) {
                    throw IllegalStateException(
                        "Found WAL entry which is out of order: expected" +
                            " transaction ID '${transactionId}', but found '${entry.transactionId}'!"
                    )
                }
                transactionActions.add(entry)
                if (entry is CommitTransactionEntry) {
                    break
                }
            }
            return transactionActions
        }

        fun readTransactionFrom(inputStream: InputStream): WriteAheadLogTransaction {
            val walEntries = readTransactionActionsFrom(inputStream)
            val commitEntry = walEntries.last() as CommitTransactionEntry
            val transactionId = commitEntry.transactionId
            val commitTimestamp = commitEntry.commitTimestamp
            val storeIdToCommands = walEntries.asSequence().mapNotNull {
                when (it) {
                    is BeginTransactionEntry -> null
                    is CommitTransactionEntry -> null
                    is DeleteEntry -> CommandAndStoreId(it.storeId, Command.del(it.key, commitTimestamp))
                    is PutEntry -> CommandAndStoreId(it.storeId, Command.put(it.key, commitTimestamp, it.value))
                }
            }.groupBy({ it.storeId }, { it.command })
            return WriteAheadLogTransaction(
                transactionId,
                commitTimestamp,
                storeIdToCommands,
                commitEntry.commitMetadata,
            )
        }

        fun writeTransaction(outputStream: OutputStream, transaction: WriteAheadLogTransaction) {
            beginTransaction(transaction.transactionId).writeTo(outputStream)
            for((storeId, commands) in transaction.storeIdToCommands){
                for(command in commands){
                    val walEntry = when (command.opCode) {
                        Command.OpCode.PUT -> put(transaction.transactionId, storeId, command.key, command.value)
                        Command.OpCode.DEL -> delete(transaction.transactionId, storeId, command.key)
                    }
                    walEntry.writeTo(outputStream)
                }
            }
            commit(transaction.transactionId, transaction.commitTimestamp, transaction.commitMetadata).writeTo(outputStream)
        }

    }

    val transactionId: UUID

    fun writeTo(outputStream: OutputStream)


    sealed interface ModifyingTransactionEntry : WriteAheadLogEntry {

        val storeId: StoreId

        val key: Bytes

    }


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
        override val transactionId: TransactionId,
        override val storeId: StoreId,
        override val key: Bytes,
        val value: Bytes,
    ) : ModifyingTransactionEntry {

        companion object {

            const val TYPE_BYTE = 10

            fun readFromWithoutTypeByte(inputStream: InputStream): PutEntry {
                val transactionId = readUUIDFrom(inputStream.readNBytes(Long.SIZE_BYTES * 2))
                val storeId = readUUIDFrom(inputStream.readNBytes(Long.SIZE_BYTES * 2))
                val key = PrefixIO.readBytes(inputStream)
                val value = PrefixIO.readBytes(inputStream)
                return PutEntry(transactionId, storeId, key, value)
            }

        }

        override fun writeTo(outputStream: OutputStream) {
            outputStream.write(TYPE_BYTE)
            outputStream.write(transactionId.toBytes())
            outputStream.write(storeId.toBytes())
            PrefixIO.writeBytes(outputStream, this.key)
            PrefixIO.writeBytes(outputStream, this.value)
        }

    }

    data class DeleteEntry(
        override val transactionId: TransactionId,
        override val storeId: StoreId,
        override val key: Bytes,
    ) : ModifyingTransactionEntry {

        companion object {

            const val TYPE_BYTE = 20

            fun readFromWithoutTypeByte(inputStream: InputStream): DeleteEntry {
                val transactionId = readUUIDFrom(inputStream.readNBytes(Long.SIZE_BYTES * 2))
                val storeId = readUUIDFrom(inputStream.readNBytes(Long.SIZE_BYTES * 2))
                val key = PrefixIO.readBytes(inputStream)
                return DeleteEntry(transactionId, storeId, key)
            }

        }

        override fun writeTo(outputStream: OutputStream) {
            outputStream.write(TYPE_BYTE)
            outputStream.write(transactionId.toBytes())
            outputStream.write(storeId.toBytes())
            PrefixIO.writeBytes(outputStream, this.key)
        }

    }

    data class CommitTransactionEntry(
        override val transactionId: TransactionId,
        val commitTimestamp: Timestamp,
        val commitMetadata: Bytes,
    ) : WriteAheadLogEntry {

        companion object {

            const val TYPE_BYTE = 110

            fun readFromWithoutTypeByte(inputStream: InputStream): CommitTransactionEntry {
                val transactionId = readUUIDFrom(inputStream.readNBytes(Long.SIZE_BYTES * 2))
                val commitTimestamp = inputStream.readLittleEndianLong()
                val commitMetadata = PrefixIO.readBytes(inputStream)
                return CommitTransactionEntry(transactionId, commitTimestamp, commitMetadata)
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
            PrefixIO.writeBytes(outputStream, commitMetadata)
        }

    }

}


