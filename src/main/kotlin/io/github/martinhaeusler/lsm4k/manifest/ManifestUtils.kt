package io.github.martinhaeusler.lsm4k.manifest

import io.github.martinhaeusler.lsm4k.api.exceptions.ManifestException
import io.github.martinhaeusler.lsm4k.manifest.operations.ManifestOperation

object ManifestUtils {

    fun manifestReplayError(operationSequenceNumber: Int, message: String): Nothing {
        throw ManifestException("Error during Manifest Replay at operation #${operationSequenceNumber}: ${message}")
    }

    fun manifestError(message: String): Nothing {
        throw ManifestException(message)
    }

    /**
     * Validates the given [condition]. If it is not fulfilled, throws a [ManifestException] with the given [lazyMessage].
     *
     * Similar to kotlin's [require], but throws a [ManifestException] instead.
     *
     * @param condition The condition to check. If `false`, an exception is thrown.
     * @param lazyMessage The lambda that produces the error message.
     *
     * @throws ManifestException if the [condition] is not fulfilled.
     */
    inline fun validateManifest(condition: Boolean, lazyMessage: () -> String) {
        if (!condition) {
            manifestError(lazyMessage())
        }
    }

    fun validateManifestSequenceNumber(manifest: Manifest, operation: ManifestOperation) {
        validateManifestReplayOperation(operation.sequenceNumber, manifest.lastAppliedOperationSequenceNumber + 1 == operation.sequenceNumber) {
            "Manifest has operation sequence number ${manifest.lastAppliedOperationSequenceNumber}," +
                " the operation has non-successor sequence number ${operation.sequenceNumber} (expected ${manifest.lastAppliedOperationSequenceNumber + 1})!"
        }
    }


    /**
     * Validates the given [condition]. If it is not fulfilled, throws a [ManifestException] with the given [lazyMessage].
     *
     * Similar to kotlin's [require], but throws a [ManifestException] instead.
     *
     * @param operationSequenceNumber The sequence number of the operation which executes the check. Will be included in the message if the check fails.
     * @param condition The condition to check. If `false`, an exception is thrown.
     * @param lazyMessage The lambda that produces the error message.
     *
     * @throws ManifestException if the [condition] is not fulfilled.
     */
    inline fun validateManifestReplayOperation(operationSequenceNumber: Int, condition: Boolean, lazyMessage: () -> String) {
        if (!condition) {
            manifestReplayError(operationSequenceNumber, lazyMessage())
        }
    }

}