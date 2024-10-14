package org.chronos.chronostore.manifest.operations

import org.chronos.chronostore.impl.annotations.PersistentClass
import org.chronos.chronostore.manifest.LSMFileInfo
import org.chronos.chronostore.manifest.Manifest
import org.chronos.chronostore.manifest.ManifestUtils.validateManifest
import org.chronos.chronostore.manifest.ManifestUtils.validateManifestReplayOperation
import org.chronos.chronostore.manifest.ManifestUtils.validateManifestSequenceNumber
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.LevelOrTierIndex
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp

/**
 * Describes a full (major) compaction.
 */
@PersistentClass(format = PersistentClass.Format.JSON, details = "Used in Manifest")
class FullCompactionOperation(
    override val sequenceNumber: Int,
    override val storeId: StoreId,
    val inputFileIndices: Set<FileIndex>,
    val outputFileIndices: Set<FileIndex>,
    val outputLevelOrTier: LevelOrTierIndex,
    override val wallClockTime: Timestamp = System.currentTimeMillis(),
) : CompactionOperation {

    companion object {

        private fun verifyFileIndicesArePositive(inputFileIndices: Set<Int>, outputFileIndices: Set<Int>) {
            validateManifest(inputFileIndices.all { it >= 0 }) {
                val offendingFileIndex = inputFileIndices.asSequence().filter { it < 0 }.min()
                "Full Compaction requires all involved input file indices to be non-negative." +
                    " Given indices: ${inputFileIndices.sorted()}, problematic file index: ${offendingFileIndex}"
            }
            validateManifest(outputFileIndices.all { it >= 0 }) {
                val offendingFileIndex = outputFileIndices.asSequence().filter { it < 0 }.min()
                "Full Compaction requires all involved output file indices to be non-negative." +
                    " Given indices: ${outputFileIndices.sorted()}, problematic file index: ${offendingFileIndex}"
            }
        }

        private fun verifyFileIndicesAreNotEmpty(inputFileIndices: Set<Int>, outputFileIndices: Set<Int>) {
            validateManifest(inputFileIndices.isNotEmpty()) {
                "Full Compaction requires input files to be not empty, but got none!"
            }
            validateManifest(outputFileIndices.isNotEmpty()) {
                "Full Compaction requires output files to be not empty, but got none!"
            }
        }

        private fun verifyOutputLevelOrTierIsNotNegative(levelOrTier: Int) {
            validateManifest(levelOrTier >= 0) {
                "Full Compaction requires its output level/tier to be greater than or equal to zero, but got: ${levelOrTier}"
            }
        }

    }

    init {
        verifyFileIndicesArePositive(inputFileIndices, outputFileIndices)
        verifyFileIndicesAreNotEmpty(inputFileIndices, outputFileIndices)
        verifyOutputLevelOrTierIsNotNegative(outputLevelOrTier)
    }

    fun getOutputLsmFileInfos(): Map<Int, LSMFileInfo> {
        return this.outputFileIndices.map { LSMFileInfo(it, this.outputLevelOrTier) }.associateBy { it.fileIndex }
    }

    override fun applyToManifest(manifest: Manifest): Manifest {
        validateManifestSequenceNumber(manifest, this)

        val storeMetadata = manifest.getStore(this.storeId)
        this.validateAllContainedFilesExistInTheStore(storeMetadata)

        val newLsmFiles = storeMetadata.lsmFiles
            // remove the file IDs which have been compacted
            .minusAll(this.inputFileIndices)
            // add the new file IDs resulting from the compaction
            .plusAll(this.getOutputLsmFileInfos())

        // override the old store info with the new one
        val newStoresMap = manifest.stores.plus(
            this.storeId,
            storeMetadata.copy(lsmFiles = newLsmFiles)
        )

        // copy the manifest object, overriding the stores map.
        return manifest.copy(
            stores = newStoresMap,
            lastAppliedOperationSequenceNumber = this.sequenceNumber,
        )
    }

    private fun validateAllContainedFilesExistInTheStore(storeMetadata: StoreMetadata) {
        validateManifestReplayOperation(this.sequenceNumber, this.inputFileIndices.all { it in storeMetadata.lsmFiles.keys }) {
            val missingLsmFileIndices = this.inputFileIndices.filter { it !in storeMetadata.lsmFiles.keys }.sorted()
            val explicitlyListed = missingLsmFileIndices.take(10)
            val andXMore = missingLsmFileIndices.asSequence().drop(10).count()
            val moreClause = if (andXMore <= 0) {
                ""
            } else {
                " and ${andXMore} more"
            }
            "Cannot apply ${FullCompactionOperation::class.simpleName}: The store does not contain" +
                " the file indices referenced by the operation." +
                " Missing in store: ${explicitlyListed}${moreClause}"
        }
    }

    override fun toString(): String {
        return "FullCompactionOperation(${this.sequenceNumber})[" +
            "at=${wallClockTime}, " +
            "on=${storeId}, " +
            "inputFiles=${inputFileIndices.size}, " +
            "outputFiles=${outputFileIndices.size}" +
            "outputLevelOrTier=${outputLevelOrTier}" +
            "]"
    }

}