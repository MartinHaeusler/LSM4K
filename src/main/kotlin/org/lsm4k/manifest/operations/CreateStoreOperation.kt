package org.lsm4k.manifest.operations

import org.lsm4k.impl.annotations.PersistentClass
import org.lsm4k.manifest.Manifest
import org.lsm4k.manifest.ManifestUtils.validateManifest
import org.lsm4k.manifest.ManifestUtils.validateManifestReplayOperation
import org.lsm4k.manifest.ManifestUtils.validateManifestSequenceNumber
import org.lsm4k.manifest.StoreMetadata
import org.lsm4k.util.Timestamp

/**
 * Creates a new store with the given settings.
 */
@PersistentClass(format = PersistentClass.Format.JSON, details = "Used in Manifest")
class CreateStoreOperation(
    override val sequenceNumber: Int,
    val storeMetadata: StoreMetadata,
    override val wallClockTime: Timestamp = System.currentTimeMillis(),
) : ManifestOperation {

    companion object {

        private fun validateNoLSMFilesInStoreMetadata(storeMetadata: StoreMetadata) {
            validateManifest(storeMetadata.lsmFiles.isEmpty()) {
                "${CreateStoreOperation::class.simpleName} expects no LSM files in its store metadata!"
            }
        }

    }

    init {
        validateNoLSMFilesInStoreMetadata(this.storeMetadata)
    }

    override fun applyToManifest(manifest: Manifest): Manifest {
        validateManifestSequenceNumber(manifest, this)

        this.validateStoreIdDoesNotExistInManifest(manifest)

        return manifest.copy(
            stores = manifest.stores.plus(this.storeMetadata.storeId, this.storeMetadata),
            lastAppliedOperationSequenceNumber = this.sequenceNumber,
        )
    }

    private fun validateStoreIdDoesNotExistInManifest(manifest: Manifest) {
        validateManifestReplayOperation(this.sequenceNumber, this.storeMetadata.storeId !in manifest.stores) {
            "Cannot apply ${CreateStoreOperation::class.simpleName}: The referenced store ID already exists in the Manifest: ${this.storeMetadata.storeId}"
        }
    }


    override fun toString(): String {
        return "CreateStoreOperation(${this.sequenceNumber})[storeId=${this.storeMetadata.storeId}, compaction=${storeMetadata.compactionStrategy::class.simpleName}]"
    }

}