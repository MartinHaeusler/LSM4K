package org.chronos.chronostore.api

import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId

interface Store {

    val isSystemInternal: Boolean
        get() = this.storeId.isSystemInternal

    val storeId: StoreId

    val retainOldVersions: Boolean

    val directory: VirtualDirectory

    val validFrom: Timestamp

    val validTo: Timestamp?

    val createdByTransactionId: TransactionId

    val lowWatermarkTimestamp: Timestamp?

    val highWatermarkTimestamp: Timestamp?

    fun hasInMemoryChanges(): Boolean

    val isTerminated: Boolean
        get() = this.validTo != null

}