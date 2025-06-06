package org.lsm4k.manifest.operations

import org.lsm4k.util.StoreId

sealed interface CompactionOperation : ManifestOperation {

    /**
     * The [StoreId] for which the compaction occurs.
     */
    val storeId: StoreId


}