package io.github.martinhaeusler.lsm4k.manifest

import io.github.martinhaeusler.lsm4k.api.compaction.CompactionStrategy
import io.github.martinhaeusler.lsm4k.api.compaction.TieredCompactionStrategy
import io.github.martinhaeusler.lsm4k.impl.StoreInfo
import io.github.martinhaeusler.lsm4k.impl.annotations.PersistentClass
import io.github.martinhaeusler.lsm4k.impl.annotations.PersistentClass.Format
import io.github.martinhaeusler.lsm4k.util.FileIndex
import io.github.martinhaeusler.lsm4k.util.LevelOrTierIndex
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.TierIndex
import org.pcollections.PMap

@PersistentClass(format = Format.JSON, details = "Used in Manifest")
data class StoreMetadata(
    /** The general store information. */
    val info: StoreInfo,
    /** The LSM files in the store, by file index. */
    val lsmFiles: PMap<FileIndex, LSMFileInfo>,
    /** The compaction strategy of the store. */
    val compactionStrategy: CompactionStrategy,
) {

    val storeId: StoreId
        get() = info.storeId

    override fun toString(): String {
        return "StoreMetadata[${info.storeId}]"
    }

    fun getFileIndicesAtTierOrLevel(levelOrTierIndex: LevelOrTierIndex): Set<FileIndex> {
        return this.lsmFiles.values.asSequence()
            .filter { it.levelOrTier == levelOrTierIndex }
            .map { it.fileIndex }
            .toSet()
    }

    fun getFileInfosAtTierOrLevel(levelOrTierIndex: LevelOrTierIndex): Set<LSMFileInfo> {
        return this.lsmFiles.values.asSequence()
            .filter { it.levelOrTier == levelOrTierIndex }
            .toSet()
    }


    fun getAllFileIndices(): Set<FileIndex> {
        return this.lsmFiles.values.asSequence().map { it.fileIndex }.toSet()
    }

    fun getAllFileInfos(): List<LSMFileInfo> {
        return this.lsmFiles.values.toList()
    }

    /**
     * Determines how many non-empty tiers are currently present in the store.
     *
     * This is only applicable for stores which use a [TieredCompactionStrategy].
     */
    fun getNumberOfNonEmptyTiers(): Int {
        assertTiered()
        return this.lsmFiles.entries.asSequence().map { it.value.levelOrTier }.toSet().size
    }


    fun getTierForFile(index: FileIndex): TierIndex {
        return this.getTierForFileOrNull(index)
            ?: throw IllegalStateException("FileIndex ${index} does not reference a known file!")
    }

    fun getTierForFileOrNull(index: FileIndex): TierIndex? {
        this.assertTiered()
        require(index >= 0) { "FileIndex must not be negative: ${index}" }
        return this.lsmFiles[index]?.levelOrTier
    }

    fun getNextHigherNonEmptyTier(tier: TierIndex): Int? {
        this.assertTiered()
        require(tier >= 0) { "TierIndex must not be negative: ${tier}" }
        return this.lsmFiles.values.asSequence()
            .map { it.levelOrTier }
            .filter { it > tier }
            .minOrNull()
    }

    fun getHighestNonEmptyLevelOrTier(): LevelOrTierIndex? {
        return this.lsmFiles.values.maxOfOrNull { it.levelOrTier }
    }


    // =================================================================================================================
    // ASSERTIONS
    // =================================================================================================================

    private fun assertTiered() {
        require(this.compactionStrategy is TieredCompactionStrategy) {
            "The store '${this.storeId}' does not use tiered compaction!"
        }
    }

}