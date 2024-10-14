package org.chronos.chronostore.manifest.operations

import org.chronos.chronostore.api.compaction.TieredCompactionStrategy
import org.chronos.chronostore.impl.annotations.PersistentClass
import org.chronos.chronostore.manifest.LSMFileInfo
import org.chronos.chronostore.manifest.Manifest
import org.chronos.chronostore.manifest.ManifestUtils
import org.chronos.chronostore.manifest.ManifestUtils.validateManifestReplayOperation
import org.chronos.chronostore.manifest.ManifestUtils.validateManifestSequenceNumber
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TierIndex
import org.chronos.chronostore.util.Timestamp

/**
 * Describes a tiered compaction.
 */
@PersistentClass(format = PersistentClass.Format.JSON, details = "Used in Manifest")
class TieredCompactionOperation(
    override val sequenceNumber: Int,
    override val storeId: StoreId,
    /** The output file indices of the compaction. */
    val outputFileIndices: Set<FileIndex>,
    /**
     *  Tells which files have been compacted.
     *
     *  The keys of the map contain the participating tiers.
     *
     *  The values of the map contain the indices of the files in that tier.
     */
    val tierToFileIndices: Map<TierIndex, Set<FileIndex>>,
    override val wallClockTime: Timestamp = System.currentTimeMillis(),
) : CompactionOperation {

    companion object {

        private fun verifyTierIndicesSizeIsAtLeast2(tierIndices: Collection<Int>) {
            ManifestUtils.validateManifest(tierIndices.size >= 2) {
                "At least two tier indices are required for a compaction! Given indices (size: ${tierIndices}): ${tierIndices}"
            }
        }

        private fun verifyTierIndicesArePositive(tierIndices: Collection<Int>) {
            ManifestUtils.validateManifest(tierIndices.all { it >= 0 }) {
                "Tier indices must not be negative! Given indices: ${tierIndices.sorted()}, " +
                    " problem occurs at: ${tierIndices.asSequence().filter { it < 0 }.min()}"
            }
        }

        private fun verifyTierIndicesAreConsecutive(tierIndices: Collection<Int>) {
            for (window in tierIndices.sorted().windowed(2)) {
                val (lowerTierIndex, higherTierIndex) = window
                ManifestUtils.validateManifest(lowerTierIndex + 1 == higherTierIndex) {
                    "Compacted tiers must be consecutive! Given indices: ${tierIndices}," +
                        " problem occurs at: ${lowerTierIndex} + 1 != ${higherTierIndex}"
                }
            }
        }

        private fun verifyEachFileIndexOccursOnlyOnce(tierFiles: Collection<Set<Int>>) {
            val seen = mutableSetOf<Int>()
            for (tier in tierFiles) {
                for (fileIndex in tier) {
                    ManifestUtils.validateManifest(fileIndex !in seen) {
                        "Compaction of tiers requires input file indices to only exist in one tier!" +
                            " File index ${fileIndex} occurs in multiple tiers!"
                    }
                    seen += fileIndex
                }
            }
        }

        private fun verifyAtLeastOneOutputFile(outputFileIndices: Set<Int>) {
            ManifestUtils.validateManifest(outputFileIndices.isNotEmpty()) {
                "Compaction of tiers requires at least one output file index, but got none!"
            }
        }

        private fun verifyFileIndicesArePositive(fileIndices: Set<Int>) {
            ManifestUtils.validateManifest(fileIndices.all { it >= 0 }) {
                "Compaction of tiers requires all involved file indices to be non-negative." +
                    " Given indices: ${fileIndices.sorted()}, problematic file index: ${fileIndices.asSequence().filter { it < 0 }.min()}"
            }
        }

        private fun verifyEachTierHasAtLeastOneFile(tierToFileIndices: Map<Int, Set<Int>>) {
            for ((tier, fileIndices) in tierToFileIndices) {
                ManifestUtils.validateManifest(fileIndices.isNotEmpty()) {
                    "Compaction of tiers requires all involved tiers to have at least one file. Tier ${tier} has no listed files!"
                }
            }
        }

    }

    init {
        val tierIndices = tierToFileIndices.keys
        verifyTierIndicesSizeIsAtLeast2(tierIndices)
        verifyTierIndicesArePositive(tierIndices)
        verifyTierIndicesAreConsecutive(tierIndices)
        verifyEachFileIndexOccursOnlyOnce(tierToFileIndices.values)
        verifyAtLeastOneOutputFile(outputFileIndices)
        verifyFileIndicesArePositive(tierToFileIndices.values.flatten().toSet())
        verifyEachTierHasAtLeastOneFile(tierToFileIndices)
    }

    override fun applyToManifest(manifest: Manifest): Manifest {
        validateManifestSequenceNumber(manifest, this)

        val storeMetadata = manifest.getStore(this.storeId)

        this.validateStoreHasTieredCompactionStrategy(storeMetadata)
        this.validateInputFilesExistAtTheCorrectTiers(storeMetadata)
        this.validateAllFilesOfInputTiersArePresentInOperation(storeMetadata)
        this.validateOutputFilesDoNotExistInTheStore(storeMetadata)

        val lsmFiles = storeMetadata.lsmFiles

        val highestTier = this.tierToFileIndices.keys.max()

        val newLsmFiles = lsmFiles
            // remove the input files
            .minusAll(this.tierToFileIndices.values.flatten())
            // add the output files
            .plusAll(this.outputFileIndices.associate { highestTier to LSMFileInfo(it, highestTier) })

        val newStoreMetadata = storeMetadata.copy(lsmFiles = newLsmFiles)

        return manifest.copy(
            stores = manifest.stores.plus(this.storeId, newStoreMetadata),
            lastAppliedOperationSequenceNumber = this.sequenceNumber
        )
    }

    private fun validateStoreHasTieredCompactionStrategy(storeMetadata: StoreMetadata) {
        validateManifestReplayOperation(this.sequenceNumber, storeMetadata.compactionStrategy is TieredCompactionStrategy) {
            "Cannot apply ${this::class.simpleName}: The store ${storeMetadata.storeId} uses a" +
                " ${storeMetadata.compactionStrategy::class.simpleName} which is incompatible with this operation" +
                " (expected a ${TieredCompactionStrategy::class.simpleName})!"
        }
    }

    private fun validateInputFilesExistAtTheCorrectTiers(storeMetadata: StoreMetadata) {
        for ((tierIndex, fileIndices) in this.tierToFileIndices) {
            val storeFileIndices = storeMetadata.getFileIndicesAtTierOrLevel(tierIndex)
            validateManifestReplayOperation(this.sequenceNumber, fileIndices.all { it in storeFileIndices }) {
                val missingFileIndices = fileIndices.filter { it !in storeFileIndices }.sorted()
                val missingFileIndicesString = missingFileIndices.joinToString(
                    limit = 10,
                    truncated = "and ${missingFileIndices.size - 10} more"
                )
                "Cannot apply ${TieredCompactionOperation::class.simpleName}: ${missingFileIndices.size} compaction input file indices referenced" +
                    " in the operation are missing in the store at tier ${tierIndex}: ${missingFileIndicesString}"
            }
        }
    }

    private fun validateOutputFilesDoNotExistInTheStore(storeMetadata: StoreMetadata) {
        validateManifestReplayOperation(this.sequenceNumber, this.outputFileIndices.none { it in storeMetadata.lsmFiles.keys }) {
            val offendingFileIndices = this.outputFileIndices.filter { it in storeMetadata.lsmFiles.keys }.sorted()
            val offendingFileIndicesStr = offendingFileIndices.joinToString(
                limit = 10,
                truncated = "and ${offendingFileIndices.size - 10} more"
            )
            "Cannot apply ${TieredCompactionOperation::class.simpleName}: ${offendingFileIndices.size} compaction output file indices" +
                " are already referenced in the store: ${offendingFileIndicesStr}"
        }
    }

    private fun validateAllFilesOfInputTiersArePresentInOperation(storeMetadata: StoreMetadata) {
        for ((tierIndex, fileIndices) in this.tierToFileIndices) {
            val storeFileIndices = storeMetadata.getFileIndicesAtTierOrLevel(tierIndex)
            val operationFileIndices = fileIndices.toSet()
            validateManifestReplayOperation(this.sequenceNumber, storeFileIndices.all { it in operationFileIndices }) {
                val missingFileIndices = storeFileIndices.filter { it !in operationFileIndices }.sorted()
                val missingFileIndicesString = missingFileIndices.joinToString(
                    limit = 10,
                    truncated = "and ${missingFileIndices.size - 10} more"
                )
                "Cannot apply ${TieredCompactionOperation::class.simpleName}: ${missingFileIndices.size} store files at Tier ${tierIndex} are" +
                    " not referenced by the operation: ${missingFileIndicesString}"
            }
        }
    }

    override fun toString(): String {
        return "TieredCompaction(${this.sequenceNumber})[" +
            "at=${this.wallClockTime}, " +
            "on=${this.storeId}, " +
            "tiers=${tierToFileIndices.keys.sorted()}" +
            "]"
    }

}