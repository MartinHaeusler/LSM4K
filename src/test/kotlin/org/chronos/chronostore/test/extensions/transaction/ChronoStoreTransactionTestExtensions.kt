package org.chronos.chronostore.test.extensions.transaction

import org.chronos.chronostore.api.TransactionBoundStore
import org.chronos.chronostore.util.Bytes

object ChronoStoreTransactionTestExtensions {


    fun TransactionBoundStore.put(key: String, value: String) {
        this.put(Bytes(key), Bytes(value))
    }

    fun TransactionBoundStore.getLatest(key: String): Bytes? {
        return this.getLatest(Bytes(key))
    }

}