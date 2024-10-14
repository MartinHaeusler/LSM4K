package org.chronos.chronostore.manifest.operations

import org.chronos.chronostore.util.StoreId

sealed interface CompactionOperation : ManifestOperation {

    /**
     * The [StoreId] for which the compaction occurs.
     */
    val storeId: StoreId


}