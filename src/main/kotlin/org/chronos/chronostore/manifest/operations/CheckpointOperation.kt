package org.chronos.chronostore.manifest.operations

import org.chronos.chronostore.impl.annotations.PersistentClass
import org.chronos.chronostore.manifest.Manifest
import org.chronos.chronostore.manifest.ManifestUtils.validateManifestReplayOperation
import org.chronos.chronostore.util.Timestamp

/**
 * A checkpoint of the entire manifest.
 *
 * Can only appear once at the start of the manifest operations file.
 */
@PersistentClass(format = PersistentClass.Format.JSON, details = "Used in Manifest")
class CheckpointOperation(
    override val sequenceNumber: Int,
    val checkpoint: Manifest,
    override val wallClockTime: Timestamp = System.currentTimeMillis(),
) : ManifestOperation {

    override fun applyToManifest(manifest: Manifest): Manifest {
        // note that for checkpoint operations we do NOT demand that the sequence number
        // of the checkpoint operation is "manifest sequence number + 1". The reason is
        // that the checkpoint operation may only occur at the START of the manifest file,
        // and it therefore MUST be applied only to an empty manifest.
        validateManifestIsEmpty(manifest)

        // we basically just accept the state from the checkpoint as the new state here.
        return this.checkpoint.copy(lastAppliedOperationSequenceNumber = this.sequenceNumber)
    }

    private fun validateManifestIsEmpty(manifest: Manifest) {
        val isEmptyManifest = manifest.lastAppliedOperationSequenceNumber == 0 && manifest.stores.isEmpty()
        validateManifestReplayOperation(this.sequenceNumber, isEmptyManifest) {
            "Cannot apply a checkpoint operation to a non-empty manifest state!"
        }
    }

    override fun toString(): String {
        return "Checkpoint(${this.sequenceNumber})"
    }

}