package org.chronos.chronostore.impl.transaction

import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.api.TransactionalStore

interface TransactionalStoreInternal: TransactionalStore {

    /** The [Store] to which this object refers.*/
    val store: Store

    /** The [ChronoStoreTransaction] to which this object is bound.*/
    val transaction: ChronoStoreTransaction

    override val isOpen: Boolean
        get() = this.transaction.isOpen

}