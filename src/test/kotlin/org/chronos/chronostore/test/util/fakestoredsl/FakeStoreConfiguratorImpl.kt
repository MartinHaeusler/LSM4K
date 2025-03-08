package org.chronos.chronostore.test.util.fakestoredsl

import org.chronos.chronostore.api.compaction.CompactionStrategy
import org.chronos.chronostore.api.compaction.LeveledCompactionStrategy
import org.chronos.chronostore.impl.StoreInfo
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.lsm.compaction.model.CompactableFile
import org.chronos.chronostore.manifest.LSMFileInfo
import org.chronos.chronostore.manifest.StoreMetadata
import org.chronos.chronostore.util.FileIndex
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.TransactionId
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