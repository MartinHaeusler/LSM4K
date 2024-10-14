package org.chronos.chronostore.impl

import org.chronos.chronostore.impl.annotations.PersistentClass
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.TransactionId

@PersistentClass(
    format = PersistentClass.Format.JSON,
    details = "Used in manifest.",
)
data class StoreInfo(
    val storeId: StoreId,
    val validFromTSN: TSN,
    val validToTSN: TSN?,
    val createdByTransactionId: TransactionId,
) {

    val terminated: Boolean
        get() = validToTSN != null

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as StoreInfo

        return storeId == other.storeId
    }

    override fun hashCode(): Int {
        return storeId.hashCode()
    }
}