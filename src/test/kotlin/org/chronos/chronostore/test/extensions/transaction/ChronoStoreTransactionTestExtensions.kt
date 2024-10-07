package org.chronos.chronostore.test.extensions.transaction

import org.chronos.chronostore.api.TransactionalReadWriteStore
import org.chronos.chronostore.api.TransactionalStore
import org.chronos.chronostore.api.TransactionalStore.Companion.withCursor
import org.chronos.chronostore.util.bytes.BasicBytes
import org.chronos.chronostore.util.bytes.Bytes

object ChronoStoreTransactionTestExtensions {


    fun TransactionalReadWriteStore.put(key: String, value: String) {
        this.put(BasicBytes(key), BasicBytes(value))
    }

    fun TransactionalReadWriteStore.delete(key: String) {
        this.delete(BasicBytes(key))
    }

    fun TransactionalStore.get(key: String): Bytes? {
        return this.get(BasicBytes(key))
    }

    val TransactionalStore.allEntries: List<Pair<Bytes, Bytes>>
        get() {
            this.withCursor { cursor ->
                if (!cursor.first()) {
                    return emptyList()
                }
                return cursor.ascendingEntrySequenceFromHere().toList()
            }
        }


}