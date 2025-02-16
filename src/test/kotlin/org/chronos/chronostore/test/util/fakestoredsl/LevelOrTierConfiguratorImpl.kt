package org.chronos.chronostore.test.util.fakestoredsl

import org.chronos.chronostore.io.format.ChronoStoreFileSettings
import org.chronos.chronostore.io.format.FileMetaData
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.lsm.LSMTreeFile
import org.chronos.chronostore.lsm.merge.model.CompactableFile
import org.chronos.chronostore.util.FileIndex

class LevelOrTierConfiguratorImpl(
    val levelOrTier: Int?,
) : LevelOrTierConfigurator {

    private val fileBuilders = mutableListOf<LsmFileConfiguratorImpl>()

    override fun file(index: FileIndex?, configure: LsmFileConfigurator.() -> Unit) {
        val configurator = LsmFileConfiguratorImpl(index)
        configure(configurator)
        this.fileBuilders += configurator
    }

    fun build(autoFileIndex: AutoIndex, directory: VirtualDirectory): List<CompactableFile> {
        return this.fileBuilders.map { fileBuilder ->
            val fileIndex = autoFileIndex.getExplicitOrNextFree(fileBuilder.index)
            directory.file("${fileIndex}.${LSMTreeFile.FILE_EXTENSION}").create()
            FakeCompactableFile(
                fileIndex, FileMetaData(
                    settings = ChronoStoreFileSettings(
                        compression = fileBuilder.compression,
                        maxBlockSize = fileBuilder.maxBlockSize,
                    ),
                    fileUUID = fileBuilder.fileUUID,
                    minKey = fileBuilder.minKey,
                    maxKey = fileBuilder.maxKey,
                    minTSN = fileBuilder.minTSN,
                    maxTSN = fileBuilder.maxTSN,
                    headEntries = fileBuilder.headEntries,
                    totalEntries = fileBuilder.totalEntries,
                    numberOfBlocks = fileBuilder.numberOfBlocks,
                    numberOfMerges = fileBuilder.numberOfMerges,
                    createdAt = fileBuilder.createdAt,
                    bloomFilter = fileBuilder.bloomFilter,
                )
            )
        }
    }
}