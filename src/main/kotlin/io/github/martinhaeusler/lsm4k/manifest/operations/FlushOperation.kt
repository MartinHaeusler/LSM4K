package io.github.martinhaeusler.lsm4k.manifest.operations

import io.github.martinhaeusler.lsm4k.api.compaction.LeveledCompactionStrategy
import io.github.martinhaeusler.lsm4k.api.compaction.TieredCompactionStrategy
import io.github.martinhaeusler.lsm4k.impl.annotations.PersistentClass
import io.github.martinhaeusler.lsm4k.manifest.LSMFileInfo
import io.github.martinhaeusler.lsm4k.manifest.Manifest
import io.github.martinhaeusler.lsm4k.manifest.ManifestUtils.validateManifest
import io.github.martinhaeusler.lsm4k.manifest.ManifestUtils.validateManifestReplayOperation
import io.github.martinhaeusler.lsm4k.manifest.ManifestUtils.validateManifestSequenceNumber
import io.github.martinhaeusler.lsm4k.manifest.StoreMetadata
import io.github.martinhaeusler.lsm4k.util.FileIndex
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.TierIndex
import io.github.martinhaeusler.lsm4k.util.Timestamp
import org.pcollections.TreePMap

/**
 * Creates a new SST file in Level 0 with the given file index.
 */
@PersistentClass(format = PersistentClass.Format.JSON, details = "Used in Manifest")
class FlushOperation(
    override val sequenceNumber: Int,
    val storeId: StoreId,
    val fileIndex: FileIndex,
    override val wallClockTime: Timestamp = System.currentTimeMillis(),
) : ManifestOperation {

    companion object {

        private fun validateFileIndexIsNotNegative(fileIndex: FileIndex) {
            validateManifest(fileIndex >= 0) {
                "Flush Operation expects a file index greater than or equal to zero, but got: ${fileIndex}"
            }
        }

    }

    init {
        validateFileIndexIsNotNegative(this.fileIndex)
    }


    override fun applyToManifest(manifest: Manifest): Manifest {
        validateManifestSequenceNumber(manifest, this)

        val storeMetadata = manifest.getStore(this.storeId)

        this.validateFileIndexDoesNotExistInStore(storeMetadata)

        // all newly flushed files always start out at level/tier zero.
        val newLsmFiles = when (storeMetadata.compactionStrategy) {
            is LeveledCompactionStrategy -> {
                // in leveled compaction, adding a new file to level 0 is
                // no problem and has no further consequences.
                storeMetadata.lsmFiles.plus(this.fileIndex, LSMFileInfo(this.fileIndex, levelOrTier = 0))
            }

            is TieredCompactionStrategy -> {
                // in tiered compaction, adding a new file to tier 0 means
                // that ALL other files get bumped one tier higher.

                // TODO [PERFORMANCE]: this operation becomes somewhat inefficient when there are many LSM files.
                val newStoreMap = mutableMapOf<TierIndex, LSMFileInfo>()

                newStoreMap[0] = LSMFileInfo(
                    fileIndex = this.fileIndex,
                    levelOrTier = 0
                )

                storeMetadata.lsmFiles.asSequence()
                    .map { (tier, fileInfo) ->
                        // increment the tier of all existing files by 1.
                        val newTier = tier + 1
                        newTier to fileInfo.copy(levelOrTier = newTier)
                    }.toMap(newStoreMap)

                TreePMap.from(newStoreMap)
            }
        }

        val newStoreMetadata = storeMetadata.copy(lsmFiles = newLsmFiles)

        return manifest.copy(
            stores = manifest.stores.plus(this.storeId, newStoreMetadata),
            lastAppliedOperationSequenceNumber = this.sequenceNumber,
        )
    }

    private fun validateFileIndexDoesNotExistInStore(storeMetadata: StoreMetadata) {
        validateManifestReplayOperation(this.sequenceNumber, this.fileIndex !in storeMetadata.lsmFiles.keys) {
            "Cannot apply ${FlushOperation::class.simpleName}: The file index ${this.fileIndex} already exists in the store!"
        }
    }

    override fun toString(): String {
        return "FlushOperation(${this.sequenceNumber})[storeId=${this.storeId}, fileIndex=${this.fileIndex}]"
    }

}