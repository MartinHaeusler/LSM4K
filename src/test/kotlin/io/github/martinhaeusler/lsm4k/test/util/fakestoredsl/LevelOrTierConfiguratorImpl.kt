package io.github.martinhaeusler.lsm4k.test.util.fakestoredsl

import io.github.martinhaeusler.lsm4k.io.format.FileMetaData
import io.github.martinhaeusler.lsm4k.io.format.LSMFileSettings
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualDirectory
import io.github.martinhaeusler.lsm4k.lsm.LSMTreeFile
import io.github.martinhaeusler.lsm4k.lsm.compaction.model.CompactableFile
import io.github.martinhaeusler.lsm4k.util.FileIndex

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
            // get the intended index for this file
            val fileIndex = autoFileIndex.getExplicitOrNextFree(fileBuilder.index)

            // create the file in the directory (it will remain empty)
            directory.file("${fileIndex}.${LSMTreeFile.FILE_EXTENSION}").create()

            FakeCompactableFile(
                index = fileIndex,
                metadata = FileMetaData(
                    settings = LSMFileSettings(
                        compression = fileBuilder.compression,
                        maxBlockSize = fileBuilder.maxBlockSize,
                    ),
                    fileUUID = fileBuilder.fileUUID,
                    minKey = fileBuilder.minKey,
                    maxKey = fileBuilder.maxKey,
                    firstKeyAndTSN = fileBuilder.firstKeyAndTSN,
                    lastKeyAndTSN = fileBuilder.lastKeyAndTSN,
                    minTSN = fileBuilder.minTSN,
                    maxTSN = fileBuilder.maxTSN,
                    headEntries = fileBuilder.headEntries,
                    totalEntries = fileBuilder.totalEntries,
                    numberOfBlocks = fileBuilder.numberOfBlocks,
                    numberOfMerges = fileBuilder.numberOfMerges,
                    createdAt = fileBuilder.createdAt,
                    bloomFilter = fileBuilder.bloomFilter,
                    maxCompletelyWrittenTSN = fileBuilder.maxCompletelyWrittenTSN,
                ),
                sizeOnDisk = fileBuilder.sizeOnDisk,
            )
        }
    }
}