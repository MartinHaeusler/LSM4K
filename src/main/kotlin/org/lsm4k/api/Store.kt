package org.lsm4k.api

import org.lsm4k.impl.StoreInfo
import org.lsm4k.io.vfs.VirtualDirectory
import org.lsm4k.util.StoreId
import org.lsm4k.util.TSN
import org.lsm4k.util.TransactionId
import org.lsm4k.util.bytes.Bytes
import java.util.concurrent.CompletableFuture

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

    val info: StoreInfo
        get() = StoreInfo(
            storeId = this.storeId,
            validFromTSN = this.validFromTSN,
            validToTSN = this.validToTSN,
            createdByTransactionId = this.createdByTransactionId,
        )

    fun scheduleMajorCompaction(): CompletableFuture<*>

    fun scheduleMinorCompaction(): CompletableFuture<*>

    fun scheduleMemtableFlush(scheduleMinorCompactionOnCompletion: Boolean): CompletableFuture<*>

}