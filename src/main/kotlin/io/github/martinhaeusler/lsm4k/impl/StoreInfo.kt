package io.github.martinhaeusler.lsm4k.impl

import io.github.martinhaeusler.lsm4k.impl.annotations.PersistentClass
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.util.TransactionId

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