package org.lsm4k.manifest

import org.lsm4k.impl.StoreInfo
import org.lsm4k.manifest.operations.ManifestOperation
import org.lsm4k.util.StoreId
import org.pcollections.PMap
import org.pcollections.TreePMap

/**
 * A [Manifest] contains the full metadata of the store.
 *
 * This includes:
 *
 * - Which files exist
 *
 * - Which files belong to which levels or tiers
 *
 * - etc.
 *
 * The [Manifest] is **immutable**.
 *
 * The corresponding [ManifestFile] contains a list of [ManifestOperation]s which eventually make
 * up the whole [Manifest]. The operation structure is necessary in order to perform incremental
 * append-only updates in the file. For more information, please see [ManifestOperation].
 */
data class Manifest(
    val stores: PMap<StoreId, StoreMetadata> = TreePMap.empty(),
    val lastAppliedOperationSequenceNumber: Int = 0,
) {

    companion object {

        fun replay(manifestOperations: Sequence<ManifestOperation>): Pair<Manifest, Int> {
            // note that we always start with an empty manifest here.
            var operationCount = 0
            val manifest = manifestOperations.fold(Manifest()) { manifest, operation ->
                operationCount++
                operation.applyToManifest(manifest)
            }
            return Pair(manifest, operationCount)
        }

    }

    val storeInfos: List<StoreInfo> by lazy {
        this.stores.values.asSequence().map(StoreMetadata::info).toList()
    }

    fun getStoreOrNull(storeId: StoreId): StoreMetadata? {
        return this.stores[storeId]
    }

    fun getStore(storeId: StoreId): StoreMetadata {
        return this.getStoreOrNull(storeId)
            ?: throw IllegalArgumentException("There is no store for ID '${storeId}'!")
    }

    fun hasStore(storeId: StoreId): Boolean {
        return storeId in this.stores
    }

}


