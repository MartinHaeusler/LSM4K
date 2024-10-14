package org.chronos.chronostore.manifest.operations

import org.chronos.chronostore.impl.annotations.PersistentClass
import org.chronos.chronostore.manifest.Manifest
import org.chronos.chronostore.manifest.ManifestUtils.validateManifest
import org.chronos.chronostore.manifest.ManifestUtils.validateManifestReplayOperation
import org.chronos.chronostore.manifest.ManifestUtils.validateManifestSequenceNumber
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.Timestamp

/**
 * Deletes an existing store.
 */
@PersistentClass(format = PersistentClass.Format.JSON, details = "Used in Manifest")
class DeleteStoreOperation(
    override val sequenceNumber: Int,
    val storeId: StoreId,
    val terminatingTSN: TSN,
    override val wallClockTime: Timestamp = System.currentTimeMillis(),
) : ManifestOperation {

    companion object {

        fun validateTerminatingTSNIsNotNegative(terminatingTSN: TSN) {
            validateManifest(terminatingTSN >= 0) {
                "Delete Store operation expects its terminating Transaction Sequence Number to be greater than or equal to zero, but got: ${terminatingTSN}"
            }
        }

    }

    init {
        validateTerminatingTSNIsNotNegative(terminatingTSN)
    }

    override fun applyToManifest(manifest: Manifest): Manifest {
        validateManifestSequenceNumber(manifest, this)

        this.validateStoreIdIsPresentInManifest(manifest)

        val newStores = manifest.stores.minus(this.storeId)

        return manifest.copy(
            stores = newStores,
            lastAppliedOperationSequenceNumber = this.sequenceNumber,
        )
    }

    private fun validateStoreIdIsPresentInManifest(manifest: Manifest) {
        validateManifestReplayOperation(this.sequenceNumber, this.storeId in manifest.stores.keys) {
            "Cannot apply ${DeleteStoreOperation::class.simpleName}: The referenced store ID '${this.storeId}' is not present in the Manifest!"
        }
    }

    override fun toString(): String {
        return "DeleteStoreOperation(${this.sequenceNumber})[storeId=${this.storeId}, terminatingTSN=${this.terminatingTSN}]"
    }

}