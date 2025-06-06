package org.lsm4k.api

import org.lsm4k.util.TSN
import org.lsm4k.util.bytes.Bytes

/**
 * The result details of performing a "get" operation with the given [key].
 */
interface GetResult {

    /** The requested key. */
    val key: Bytes

    /**
     * Returns `true` if there was an entry in the store for the [key], or `false` if the key was completely absent from the store.
     *
     * Please keep in mind:
     *
     * - Even if this method returns `true`, the most recent operation on the [key] may have been a deletion. So even
     *   if this method returns `true`, [value] may still be `null`!
     *
     * - If the [key] has been modified as part of the ongoing transaction (i.e. it is a [transient modification][isModifiedInTransactionContext]),
     *   this method will return `true`, even though the persistent store does not necessarily contain the key.
     */
    val isHit: Boolean

    /**
     * Returns `true` if the [value] associated with the [key] has been assigned in the current transaction context ("transient modification")
     * or `false` if the value was fetched from the persistent store.
     */
    val isModifiedInTransactionContext: Boolean

    /**
     * Returns the [TSN] at which the last modification has occurred on the key (which may have been an overwrite or a deletion).
     *
     * If the key was absent from the store or if the value originates from a [transient modification][isModifiedInTransactionContext],
     * `null` will be returned instead.
     */
    val lastModificationTSN: TSN?

    /** The value associated with the [key]. */
    val value: Bytes?

}