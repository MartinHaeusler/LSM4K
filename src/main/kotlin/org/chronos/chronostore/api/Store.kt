package org.chronos.chronostore.api

import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId

interface Store {

    val isSystemInternal: Boolean
        get() = this.name.isSystemInternal

    val name: StoreId

    val retainOldVersions: Boolean

    val directory: VirtualDirectory

    val validFrom: Timestamp

    val validTo: Timestamp?

    val createdByTransactionId: TransactionId

    val isTerminated: Boolean
        get() = this.validTo != null

}