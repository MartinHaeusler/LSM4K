package org.lsm4k.manifest.operations

import org.lsm4k.api.compaction.LeveledCompactionStrategy
import org.lsm4k.impl.annotations.PersistentClass
import org.lsm4k.manifest.LSMFileInfo
import org.lsm4k.manifest.Manifest
import org.lsm4k.manifest.ManifestUtils.validateManifest
import org.lsm4k.manifest.ManifestUtils.validateManifestReplayOperation
import org.lsm4k.manifest.ManifestUtils.validateManifestSequenceNumber
import org.lsm4k.manifest.StoreMetadata
import org.lsm4k.util.FileIndex
import org.lsm4k.util.LevelIndex
import org.lsm4k.util.StoreId
import org.lsm4k.util.Timestamp

/**
 * Describes a leveled compaction.
 */
@PersistentClass(format = PersistentClass.Format.JSON, details = "Used in Manifest")
class LeveledCompactionOperation(
    override val sequenceNumber: Int,
    override val storeId: StoreId,
    /** The output file indices of the compaction. */
    val outputFileIndices: Set<FileIndex>,
    /** The level at which the output files will be placed. */
    val outputLevelIndex: LevelIndex,
    /** The upper (higher) level which is targeted by the compaction. */
    val upperLevelIndex: LevelIndex,
    /** The file indices in the upper (higher) level which will be affected by the compaction. */
    val upperLevelFileIndices: Set<FileIndex>,
    /** The lower level which is targeted by the compaction. */
    val lowerLevelIndex: LevelIndex,
    /** The file indices in the lower level which will be affected by the compaction. */
    val lowerLevelFileIndices: Set<FileIndex>,
    override val wallClockTime: Timestamp = System.currentTimeMillis(),
) : CompactionOperation {

    companion object {

        private fun verifyLevelsArePositive(lowerLevelIndex: LevelIndex, upperLevelIndex: LevelIndex, outputLevel: LevelIndex) {
            validateManifest(lowerLevelIndex >= 0) {
                "Compaction of levels requires positive level indices. The given lower level index is negative: ${lowerLevelIndex}"
            }
            validateManifest(upperLevelIndex >= 0) {
                "Compaction of levels requires positive level indices. The given upper level index is negative: ${lowerLevelIndex}"
            }
            validateManifest(outputLevel >= 0) {
                "Compaction of levels requires positive level indices. The given output level index is negative: ${outputLevel}"
            }
        }

        private fun verifyOutputLevelIsGreaterThanInputLevels(lowerLevelIndex: LevelIndex, upperLevelIndex: LevelIndex, outputLevel: LevelIndex) {
            validateManifest(outputLevel >= upperLevelIndex) {
                "Compaction of levels requires the output level (${outputLevel}) to be greater than or equal to the upper level (${upperLevelIndex})!"
            }
            validateManifest(outputLevel >= lowerLevelIndex) {
                "Compaction of levels requires the output level (${outputLevel}) to be greater than or equal to the lower level (${lowerLevelIndex})!"
            }
            validateManifest(upperLevelIndex >= lowerLevelIndex) {
                "Compaction of levels requires the upper level (${lowerLevelIndex}) to be greater than or equal to the lower level (${lowerLevelIndex})!"
            }
        }

        private fun verifyAtLeastOneOutputFile(outputFileIndices: Set<FileIndex>) {
            validateManifest(outputFileIndices.isNotEmpty()) {
                "Compaction of levels requires at least one output file index, but got none!"
            }
        }

        private fun verifyLowerLevelFileIndicesAreNotEmpty(lowerLevelFileIndices: Set<FileIndex>) {
            validateManifest(lowerLevelFileIndices.isNotEmpty()) {
                "Compaction of levels requires the lower level file indices to be non-empty, but got none!"
            }
        }

        private fun verifyFileIndicesArePositive(lowerLevelFileIndices: Set<FileIndex>, upperLevelFileIndices: Set<FileIndex>) {
            validateManifest(lowerLevelFileIndices.all { it >= 0 }) {
                "Compaction of levels requires all lower level file indices to be non-negative." +
                    " Given indices: ${lowerLevelFileIndices}, problematic file index: ${lowerLevelFileIndices.asSequence().filter { it < 0 }.min()}"
            }
            validateManifest(upperLevelFileIndices.all { it >= 0 }) {
                "Compaction of levels requires all upper level file indices to be non-negative." +
                    " Given indices: ${upperLevelFileIndices}, problematic file index: ${upperLevelFileIndices.asSequence().filter { it < 0 }.min()}"
            }
        }

        private fun verifyFileIndicesAreDisjoint(lowerLevelFileIndices: Set<FileIndex>, upperLevelFileIndices: Set<FileIndex>) {
            validateManifest(lowerLevelFileIndices.none { it in upperLevelFileIndices }) {
                val jointFileIndices = lowerLevelFileIndices.intersect(upperLevelFileIndices)
                "Compaction of levels requires file indices at lower and upper level to be disjiont." +
                    " Given indices: Lower: ${lowerLevelFileIndices.sorted()}, Upper: ${upperLevelFileIndices.sorted()}. " +
                    " The following ${jointFileIndices.size} file indices occur in both: ${jointFileIndices.sorted()}"
            }
        }

    }

    init {
        verifyLevelsArePositive(lowerLevelIndex, upperLevelIndex, outputLevelIndex)
        verifyOutputLevelIsGreaterThanInputLevels(lowerLevelIndex, upperLevelIndex, outputLevelIndex)
        // note that, while we always need files in the lower level, it is allowed for the upper-level files
        // to be empty. This is the case if the upper level doesn't contain ANY files yet.
        verifyLowerLevelFileIndicesAreNotEmpty(lowerLevelFileIndices)
        verifyFileIndicesArePositive(lowerLevelFileIndices, upperLevelFileIndices)
        verifyAtLeastOneOutputFile(outputFileIndices)
        verifyFileIndicesAreDisjoint(lowerLevelFileIndices, upperLevelFileIndices)
    }

    override fun applyToManifest(manifest: Manifest): Manifest {
        validateManifestSequenceNumber(manifest, this)

        val storeMetadata = manifest.getStore(this.storeId)
        this.validateStoreHasLeveledCompactionStrategy(storeMetadata)
        this.validateAllLowerLevelFileIndicesAreKnown(storeMetadata)
        this.validateAllLowerLevelFilesAreAtTheCorrectLevelIndex(storeMetadata)
        this.validateAllUpperLevelFileIndicesAreKnown(storeMetadata)
        this.validateAllUpperLevelFilesAreAtTheCorrectLevelIndex(storeMetadata)
        this.validateOutputFileIndicesAreNotInTheStore(storeMetadata)

        val lsmFiles = storeMetadata.lsmFiles

        val newLsmFiles = lsmFiles
            // remove the files we're compacting...
            .minusAll(this.lowerLevelFileIndices)
            .minusAll(this.upperLevelFileIndices)
            // ... and add the compaction target files at the right level
            .plusAll(this.outputFileIndices.associateWith { LSMFileInfo(it, this.outputLevelIndex) })

        val newStoreMetadata = storeMetadata.copy(lsmFiles = newLsmFiles)

        return manifest.copy(
            stores = manifest.stores.plus(this.storeId, newStoreMetadata),
            lastAppliedOperationSequenceNumber = this.sequenceNumber,
        )
    }

    private fun validateStoreHasLeveledCompactionStrategy(storeMetadata: StoreMetadata) {
        validateManifestReplayOperation(this.sequenceNumber, storeMetadata.compactionStrategy is LeveledCompactionStrategy) {
            "Cannot apply ${this::class.simpleName}: The store ${storeMetadata.storeId} uses a" +
                " ${storeMetadata.compactionStrategy::class.simpleName} which is incompatible with this operation" +
                " (expected a ${LeveledCompactionStrategy::class.simpleName})!"
        }
    }

    private fun validateAllLowerLevelFilesAreAtTheCorrectLevelIndex(storeMetadata: StoreMetadata) {
        val allLowerLevelFilesAreAtTheCorrectLevelIndex = this.lowerLevelFileIndices.all {
            storeMetadata.lsmFiles[it]?.levelOrTier == this.lowerLevelIndex
        }
        validateManifestReplayOperation(this.sequenceNumber, allLowerLevelFilesAreAtTheCorrectLevelIndex) {
            val offendingIndices = this.lowerLevelFileIndices.asSequence()
                .filter { storeMetadata.lsmFiles[it]?.levelOrTier != this.lowerLevelIndex }
                .joinToString { "${it} (L${storeMetadata.lsmFiles[it]?.levelOrTier ?: "?"})" }
            "Cannot apply ${this::class.simpleName}: The store references a different lower-level" +
                " index than stated in the operation (L${this.lowerLevelIndex}) for the following files: ${offendingIndices}"
        }
    }

    private fun validateAllUpperLevelFilesAreAtTheCorrectLevelIndex(
        storeMetadata: StoreMetadata,
    ) {
        val allUpperLevelFilesAreAtTheCorrectLevelIndex = this.upperLevelFileIndices.all {
            storeMetadata.lsmFiles[it]?.levelOrTier == this.upperLevelIndex
        }
        validateManifestReplayOperation(this.sequenceNumber, allUpperLevelFilesAreAtTheCorrectLevelIndex) {
            val offendingIndices = this.upperLevelFileIndices.asSequence()
                .filter { storeMetadata.lsmFiles[it]?.levelOrTier != this.upperLevelIndex }
                .joinToString { "${it} (L${storeMetadata.lsmFiles[it]?.levelOrTier ?: "?"})" }
            "Cannot apply ${this::class.simpleName}: The store references a different upper-level" +
                " index than stated in the operation (L${this.upperLevelIndex}) for the following files: ${offendingIndices}"
        }
    }

    private fun validateAllLowerLevelFileIndicesAreKnown(storeMetadata: StoreMetadata) {
        val allLowerLevelFileIndicesAreKnown = this.lowerLevelFileIndices.all { it in storeMetadata.lsmFiles.keys }
        validateManifestReplayOperation(this.sequenceNumber, allLowerLevelFileIndicesAreKnown) {
            val offendingIndices = this.lowerLevelFileIndices.filter { it !in storeMetadata.lsmFiles.keys }
            "Cannot apply ${this::class.simpleName}: The store does not contain the following lower-level" +
                " file indices referenced in the operation: ${offendingIndices}"
        }
    }

    private fun validateAllUpperLevelFileIndicesAreKnown(storeMetadata: StoreMetadata) {
        val allUpperLevelFileIndicesAreKnown = this.upperLevelFileIndices.all { it in storeMetadata.lsmFiles.keys }
        validateManifestReplayOperation(this.sequenceNumber, allUpperLevelFileIndicesAreKnown) {
            val offendingIndices = this.upperLevelFileIndices.filter { it !in storeMetadata.lsmFiles.keys }
            "Cannot apply ${this::class.simpleName}: The store does not contain the following upper-level" +
                " file indices referenced in the operation: ${offendingIndices}"
        }
    }

    private fun validateOutputFileIndicesAreNotInTheStore(storeMetadata: StoreMetadata) {
        val outputFileIndicesAreNotInTheStore = this.outputFileIndices.none { it in storeMetadata.lsmFiles.keys }
        validateManifestReplayOperation(this.sequenceNumber, outputFileIndicesAreNotInTheStore) {
            val offendingIndices = this.outputFileIndices.filter { it in storeMetadata.lsmFiles.keys }
            "Cannot apply  ${this::class.simpleName}: The store already contains some of the output" +
                " file indices referenced in the operation: ${offendingIndices}"
        }
    }


    override fun toString(): String {
        return "LeveledCompaction(${this.sequenceNumber})[" +
            "at=${this.wallClockTime}, " +
            "on=${this.storeId}, " +
            "lowerLevel=${lowerLevelIndex} (${lowerLevelFileIndices.size} files), " +
            "upperLevel=${upperLevelIndex} (${upperLevelFileIndices.size})" +
            "outputLevel=${outputLevelIndex}" +
            "]"
    }
}