package org.chronos.chronostore.impl.transaction

import org.chronos.chronostore.api.GetResult
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.impl.store.StoreImpl
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.IndexBasedCursor
import org.chronos.chronostore.util.cursor.OverlayCursor

class TransactionalStoreImpl(
    override val transaction: ChronoStoreTransactionImpl,
    override val store: Store,
) : TransactionalReadWriteStoreInternal {

    override val storeId: StoreId
        get() = this.store.storeId

    override val isVersioned: Boolean
        get() = this.store.retainOldVersions

    override fun put(key: Bytes, value: Bytes) {
        this.transactionContext.performPut(key, value)
    }

    override fun delete(key: Bytes) {
        this.transactionContext.performDelete(key)
    }

    override fun getLatest(key: Bytes): Bytes? {
        return if (this.transactionContext.isKeyModified(key)) {
            this.transactionContext.getLatest(key)
        } else {
            val command = (this.store as StoreImpl).tree.get(KeyAndTimestamp(key, this.transaction.lastVisibleTimestamp))
            when (command?.opCode) {
                Command.OpCode.DEL, null -> null
                Command.OpCode.PUT -> command.value
            }
        }
    }

    override fun getLatestWithDetails(key: Bytes): GetResult {
        return if (this.transactionContext.isKeyModified(key)) {
            GetResultImpl(
                key = key,
                isHit = true,
                isModifiedInTransactionContext = true,
                lastModifiedAtTimestamp = null,
                value = this.transactionContext.getLatest(key)
            )
        } else {
            val command = (this.store as StoreImpl).tree.get(KeyAndTimestamp(key, this.transaction.lastVisibleTimestamp))
            GetResultImpl(
                key = key,
                isHit = command != null,
                isModifiedInTransactionContext = false,
                lastModifiedAtTimestamp = command?.timestamp,
                value = when (command?.opCode) {
                    Command.OpCode.DEL, null -> null
                    Command.OpCode.PUT -> command.value
                }
            )
        }
    }

    override fun getAtTimestamp(key: Bytes, timestamp: Timestamp): Bytes? {
        check(this.isVersioned) {
            "Method 'getAtTimestamp(...)' is only allowed on versioned stores. Store '${this.storeId}' is not versioned." +
                " Please use method 'getLatest(...)' instead."
        }
        val now = this.transaction.lastVisibleTimestamp
        require(timestamp <= now) {
            "Cannot query key at timestamp ${timestamp}: it is later than the last visible timestamp ($now)!"
        }
        val command = (this.store as StoreImpl).tree.get(KeyAndTimestamp(key, now))
        return when (command?.opCode) {
            Command.OpCode.DEL, null -> null
            Command.OpCode.PUT -> command.value
        }
    }

    override fun getAtTimestampWithDetails(key: Bytes, timestamp: Timestamp): GetResult {
        check(this.isVersioned) {
            "Method 'getAtTimestampWithDetails(...)' is only allowed on versioned stores. Store '${this.storeId}' is not versioned." +
                " Please use method 'getLatestWithDetails(...)' instead."
        }

        val now = this.transaction.lastVisibleTimestamp
        require(timestamp <= now) {
            "Cannot query key at timestamp ${timestamp}: it is later than the last visible timestamp ($now)!"
        }
        val command = (this.store as StoreImpl).tree.get(KeyAndTimestamp(key, now))
        return GetResultImpl(
            key = key,
            isHit = command != null,
            isModifiedInTransactionContext = false,
            lastModifiedAtTimestamp = command?.timestamp,
            value = when (command?.opCode) {
                Command.OpCode.DEL, null -> null
                Command.OpCode.PUT -> command.value
            }
        )
    }

    override fun openCursorOnLatest(): Cursor<Bytes, Bytes> {
        val treeCursor = (this.store as StoreImpl).tree.openCursor(this.transaction, this.transaction.lastVisibleTimestamp)
        val bytesToBytesCursor = treeCursor.filterValues { it != null && it.opCode != Command.OpCode.DEL }.mapValue { it.value }
        val transientModifications = this.transactionContext.allModifications.entries.asSequence().mapNotNull {
            val key = it.key
            val value = it.value
                ?: return@mapNotNull null
            key to value
        }.toMutableList().sortedBy { it.first }
        val transientModificationCursor = if (transientModifications.isNotEmpty()) {
            IndexBasedCursor(
                minIndex = 0,
                maxIndex = transientModifications.lastIndex,
                getEntryAtIndex = { transientModifications[it] },
                getCursorName = { "Transient Modification Cursor" }
            )
        } else {
            return bytesToBytesCursor
        }
        return OverlayCursor(bytesToBytesCursor, transientModificationCursor)
    }

    override fun openCursorAtTimestamp(timestamp: Timestamp): Cursor<Bytes, Bytes> {
        val now = this.transaction.lastVisibleTimestamp
        require(timestamp in (0..now)) {
            "The given timestamp (${timestamp}) is out of range. Expected a positive" +
                " number less than or equal to the transaction timestamp (${now})!"
        }
        // transient modifications are ignored on historical queries.
        val treeCursor = (this.store as StoreImpl).tree.openCursor(this.transaction, timestamp)
        return treeCursor.filterValues { it != null && it.opCode != Command.OpCode.DEL }.mapValue { it.value }
    }

    override fun deleteStore() {
        this.transaction.deleteStore(this)
        this.transactionContext.clearModifications()
    }

    val transactionContext = TransactionBoundStoreContext(this.store)

    override fun toString(): String {
        return "TransactionBoundStore[${this.storeId}]"
    }

}