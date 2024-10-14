package org.chronos.chronostore.manifest.operations

import org.chronos.chronostore.api.compaction.LeveledCompactionStrategy
import org.chronos.chronostore.api.compaction.TieredCompactionStrategy
import org.chronos.chronostore.impl.annotations.PersistentClass
import org.chronos.chronostore.manifest.LSMFileInfo
import org.chronos.chronostore.manifest.Manifest
import org.chronos.chronostore.manifest.ManifestUtils.validateManifest
import org.chronos.chronostore.manifest.ManifestUtils.validateManifestReplayOperation
import org.chronos.chronostore.manifest.ManifestUtils.validateManifestSequenceNumber
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TierIndex
import org.chronos.chronostore.util.Timestamp
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