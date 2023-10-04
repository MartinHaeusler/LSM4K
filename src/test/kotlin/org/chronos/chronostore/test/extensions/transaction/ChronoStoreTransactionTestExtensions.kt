package org.chronos.chronostore.test.extensions.transaction

import org.chronos.chronostore.api.TransactionBoundStore
import org.chronos.chronostore.util.bytes.BasicBytes
import org.chronos.chronostore.util.bytes.Bytes

object ChronoStoreTransactionTestExtensions {


    fun TransactionBoundStore.put(key: String, value: String) {
        this.put(BasicBytes(key), BasicBytes(value))
    }

    fun TransactionBoundStore.delete(key: String) {
        this.delete(BasicBytes(key))
    }

    fun TransactionBoundStore.getLatest(key: String): Bytes? {
        return this.getLatest(BasicBytes(key))
    }

    val TransactionBoundStore.allEntriesOnLatest: List<Pair<Bytes, Bytes>>
        get() {
            this.openCursorOnLatest().use { cursor ->
                if(!cursor.first()){
                    return emptyList()
                }
                return cursor.ascendingEntrySequenceFromHere().toList()
            }
        }


}