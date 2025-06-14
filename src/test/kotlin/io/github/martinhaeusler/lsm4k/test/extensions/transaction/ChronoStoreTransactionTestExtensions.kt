package io.github.martinhaeusler.lsm4k.test.extensions.transaction

import io.github.martinhaeusler.lsm4k.api.TransactionalReadWriteStore
import io.github.martinhaeusler.lsm4k.api.TransactionalStore
import io.github.martinhaeusler.lsm4k.api.TransactionalStore.Companion.withCursor
import io.github.martinhaeusler.lsm4k.util.bytes.BasicBytes
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes

object LSM4KTransactionTestExtensions {


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