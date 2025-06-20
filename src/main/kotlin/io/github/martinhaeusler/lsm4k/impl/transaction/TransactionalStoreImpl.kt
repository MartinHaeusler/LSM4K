package io.github.martinhaeusler.lsm4k.impl.transaction

import io.github.martinhaeusler.lsm4k.api.GetResult
import io.github.martinhaeusler.lsm4k.api.Store
import io.github.martinhaeusler.lsm4k.api.TransactionalStore.Companion.withCursor
import io.github.martinhaeusler.lsm4k.impl.store.StoreImpl
import io.github.martinhaeusler.lsm4k.lsm.LSMTree
import io.github.martinhaeusler.lsm4k.model.command.KeyAndTSN
import io.github.martinhaeusler.lsm4k.model.command.OpCode
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.cursor.Cursor
import io.github.martinhaeusler.lsm4k.util.cursor.IndexBasedCursor
import io.github.martinhaeusler.lsm4k.util.cursor.OverlayCursor.Companion.overlayOnto
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsReporter
import java.util.*

class TransactionalStoreImpl(
    override val transaction: LSM4KTransactionImpl,
    override val store: Store,
    private val statisticsReporter: StatisticsReporter,
) : TransactionalReadWriteStoreInternal {

    // TODO [SCALABILITY]: A transaction should be able to "spill" its transient modifications to disk if they get too big.
    //                     This is typically done using a memory limit (e.g. 100MiB, configurable) per transaction. When the
    //                     transaction context exceeds this limit, it is written to a temporary file instead. This is hard to
    //                     do here at the moment because we have one context *per store*. We should centralize the context such
    //                     that we have a single context attached to the transaction.
    val transactionContext = TransactionBoundStoreContext(this.store)

    override val storeId: StoreId
        get() = this.store.storeId

    override fun put(key: Bytes, value: Bytes) {
        this.transactionContext.performPut(key, value)
    }

    override fun delete(key: Bytes) {
        this.transactionContext.performDelete(key)
    }

    override fun get(key: Bytes): Bytes? {
        return if (this.transactionContext.isDirty() && this.transactionContext.isKeyModified(key)) {
            this.transactionContext.getLatest(key)
        } else {
            val keyAndTSN = KeyAndTSN(key, this.transaction.lastVisibleSerialNumber)
            val command = this.getTree().getLatestVersion(keyAndTSN)
            when (command?.opCode) {
                OpCode.DEL, null -> null
                OpCode.PUT -> command.value
            }
        }
    }

    override fun getMultiple(keys: Iterable<Bytes>): Map<Bytes, Bytes> {
        val tree = this.getTree()
        val ascendingSortedKeys = keys.toSortedSet()

        //TODO [PERFORMANCE]: This needs some benchmarking to find out if 10% is the right number.

        // if we need more than every 10th key, it's likely more
        // efficient to use a cursor rather than getting them one by one.
        val threshold = tree.estimatedNumberOfEntries / 10

        return if (ascendingSortedKeys.size > threshold) {
            getMultipleWithCursor(ascendingSortedKeys)
        } else {
            getMultipleOneByOne(ascendingSortedKeys)
        }
    }

    private fun getMultipleOneByOne(ascendingSortedKeys: SortedSet<Bytes>): Map<Bytes, Bytes> {
        return ascendingSortedKeys.asSequence().mapNotNull { key ->
            val value = this.get(key)
                ?: return@mapNotNull null
            key to value
        }.toMap()
    }

    private fun getMultipleWithCursor(ascendingSortedKeys: SortedSet<Bytes>): Map<Bytes, Bytes> {
        this.withCursor { cursor ->
            val resultMap = mutableMapOf<Bytes, Bytes>()

            for (key in ascendingSortedKeys) {
                if (!cursor.seekExactlyOrNext(key)) {
                    // the cursor is out of keys!
                    return resultMap
                }
                // did we find our key exactly?
                if (cursor.key == key) {
                    resultMap[key] = cursor.value
                }
            }

            // we've checked all requested keys; stop here.
            return resultMap
        }
    }

    override fun getWithDetails(key: Bytes): GetResult {
        return if (this.transactionContext.isKeyModified(key)) {
            GetResultImpl(
                key = key,
                isHit = true,
                isModifiedInTransactionContext = true,
                lastModificationTSN = null,
                value = this.transactionContext.getLatest(key)
            )
        } else {
            val command = getTree().getLatestVersion(KeyAndTSN(key, this.transaction.lastVisibleSerialNumber))
            GetResultImpl(
                key = key,
                isHit = command != null,
                isModifiedInTransactionContext = false,
                lastModificationTSN = command?.tsn,
                value = when (command?.opCode) {
                    OpCode.DEL, null -> null
                    OpCode.PUT -> command.value
                }
            )
        }
    }


    override fun openCursor(): Cursor<Bytes, Bytes> {
        val treeCursor = this.getTree().openCursor(this.transaction)
        val bytesToBytesCursor = treeCursor.filterValues { it != null && it.opCode != OpCode.DEL }.mapValue { it.value }
        val transientModifications = this.transactionContext.allModifications.entries.asSequence().map {
            val key = it.key
            val value = it.value
            key to value
        }.toMutableList().sortedBy { it.first }
        val transientModificationCursor = if (transientModifications.isNotEmpty()) {
            IndexBasedCursor(
                minIndex = 0,
                maxIndex = transientModifications.lastIndex,
                getEntryAtIndex = { transientModifications[it] },
                name = "Transient Modification Cursor"
            )
        } else {
            return bytesToBytesCursor
        }
        return transientModificationCursor.overlayOnto(bytesToBytesCursor, this.statisticsReporter)
    }

    private fun getTree(): LSMTree {
        return (this.store as StoreImpl).tree
    }

    override fun deleteStore() {
        this.transaction.deleteStore(this)
        this.transactionContext.clearModifications()
    }


    override fun toString(): String {
        return "TransactionBoundStore[${this.storeId}]"
    }

}