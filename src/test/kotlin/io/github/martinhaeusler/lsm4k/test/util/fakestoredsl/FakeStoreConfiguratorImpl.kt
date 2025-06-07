package io.github.martinhaeusler.lsm4k.test.util.fakestoredsl

import io.github.martinhaeusler.lsm4k.api.compaction.CompactionStrategy
import io.github.martinhaeusler.lsm4k.api.compaction.LeveledCompactionStrategy
import io.github.martinhaeusler.lsm4k.impl.StoreInfo
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualDirectory
import io.github.martinhaeusler.lsm4k.lsm.compaction.model.CompactableFile
import io.github.martinhaeusler.lsm4k.manifest.LSMFileInfo
import io.github.martinhaeusler.lsm4k.manifest.StoreMetadata
import io.github.martinhaeusler.lsm4k.util.FileIndex
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.util.TransactionId
import org.pcollections.TreePMap

class FakeStoreConfiguratorImpl(
    val storeId: StoreId,
    val directory: VirtualDirectory,
) : FakeStoreConfigurator {

    // =================================================================================================================
    // INTERNAL BUILDER STATE
    // =================================================================================================================

    private val levelsOrTiersBuilders = mutableListOf<LevelOrTierConfiguratorImpl>()

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    override var validFromTSN: TSN = 0L

    override var validToTSN: TSN? = null

    override var createdByTransactionId: TransactionId = TransactionId.randomUUID()

    override var compactionStrategy: CompactionStrategy = LeveledCompactionStrategy()

    override fun level(level: Int?, configure: LevelOrTierConfigurator.() -> Unit) {
        val configurator = LevelOrTierConfiguratorImpl(level)
        configure(configurator)
        this.levelsOrTiersBuilders += configurator
    }

    override fun tier(tier: Int?, configure: LevelOrTierConfigurator.() -> Unit) {
        val configurator = LevelOrTierConfiguratorImpl(tier)
        configure(configurator)
        this.levelsOrTiersBuilders += configurator
    }

    // =================================================================================================================
    // BUILD
    // =================================================================================================================

    fun build(): FakeCompactableStore {
        val autoLevelIndex = AutoIndex()
        val autoFileIndex = AutoIndex()

        val allFiles = mutableListOf<CompactableFile>()
        val fileIndexToInfo = mutableMapOf<FileIndex, LSMFileInfo>()

        for (levelOrTierBuilder in this.levelsOrTiersBuilders) {
            val effectiveLevel = autoLevelIndex.getExplicitOrNextFree(levelOrTierBuilder.levelOrTier)
            val files = levelOrTierBuilder.build(
                autoFileIndex = autoFileIndex,
                directory = this.directory,
            )

            for (file in files) {
                val fileIndex = file.index
                if (fileIndexToInfo.containsKey(fileIndex)) {
                    throw IllegalStateException(
                        "File index '$fileIndex' is assigned twice!" +
                            " Please use unique file indices in the builder or use auto-indices."
                    )
                }
                val fileInfo = LSMFileInfo(fileIndex, effectiveLevel)
                fileIndexToInfo[fileIndex] = fileInfo
            }
            allFiles += files
        }

        return FakeCompactableStore(
            allFiles = allFiles,
            metadata = StoreMetadata(
                info = StoreInfo(
                    storeId = this.storeId,
                    validFromTSN = this.validFromTSN,
                    validToTSN = this.validToTSN,
                    createdByTransactionId = this.createdByTransactionId,
                ),
                lsmFiles = TreePMap.from(fileIndexToInfo),
                compactionStrategy = this.compactionStrategy,
            )
        )
    }

}