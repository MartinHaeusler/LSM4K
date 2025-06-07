package io.github.martinhaeusler.lsm4k.impl.transaction

import io.github.martinhaeusler.lsm4k.api.LSM4KTransaction
import io.github.martinhaeusler.lsm4k.api.Store
import io.github.martinhaeusler.lsm4k.api.TransactionalStore

interface TransactionalStoreInternal : TransactionalStore {

    /** The [Store] to which this object refers.*/
    val store: Store

    /** The [LSM4KTransaction] to which this object is bound.*/
    val transaction: LSM4KTransaction

    override val isOpen: Boolean
        get() = this.transaction.isOpen

}