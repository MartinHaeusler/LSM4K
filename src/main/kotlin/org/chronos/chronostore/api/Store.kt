package org.chronos.chronostore.api

import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.bytes.Bytes

/**
 * A [Store] binds an ordered set of [Bytes] keys to a set of [Bytes] values.
 *
 * A store is always bound to a [storeId] and a [directory] which holds the data.
 *
 * A store has a validity range represented by [validFromTSN] and [validToTSN] (potentially open-ended, represented as [validToTSN]` == null`).
 */
interface Store {

    val isSystemInternal: Boolean
        get() = this.storeId.isSystemInternal

    val storeId: StoreId

    val directory: VirtualDirectory

    val validFromTSN: TSN

    val validToTSN: TSN?

    val createdByTransactionId: TransactionId

    val lowWatermarkTSN: TSN?

    val highWatermarkTSN: TSN?

    fun hasInMemoryChanges(): Boolean

    val isTerminated: Boolean
        get() = this.validToTSN != null

}