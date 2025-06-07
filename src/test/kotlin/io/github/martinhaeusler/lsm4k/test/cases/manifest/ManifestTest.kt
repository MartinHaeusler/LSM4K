package io.github.martinhaeusler.lsm4k.test.cases.manifest

import io.github.martinhaeusler.lsm4k.api.compaction.CompactionStrategy
import io.github.martinhaeusler.lsm4k.api.compaction.LeveledCompactionStrategy
import io.github.martinhaeusler.lsm4k.api.compaction.TieredCompactionStrategy
import io.github.martinhaeusler.lsm4k.impl.StoreInfo
import io.github.martinhaeusler.lsm4k.manifest.LSMFileInfo
import io.github.martinhaeusler.lsm4k.manifest.Manifest
import io.github.martinhaeusler.lsm4k.manifest.ManifestFile
import io.github.martinhaeusler.lsm4k.manifest.StoreMetadata
import io.github.martinhaeusler.lsm4k.manifest.operations.CreateStoreOperation
import io.github.martinhaeusler.lsm4k.manifest.operations.FlushOperation
import io.github.martinhaeusler.lsm4k.manifest.operations.LeveledCompactionOperation
import io.github.martinhaeusler.lsm4k.manifest.operations.TieredCompactionOperation
import io.github.martinhaeusler.lsm4k.test.util.VFSMode
import io.github.martinhaeusler.lsm4k.test.util.VirtualFileSystemTest
import io.github.martinhaeusler.lsm4k.util.*
import io.github.martinhaeusler.lsm4k.util.IOExtensions.withInputStream
import org.pcollections.PMap
import org.pcollections.TreePMap
import strikt.api.Assertion
import strikt.api.expectThat
import strikt.assertions.*

class ManifestTest {

    @VirtualFileSystemTest
    fun canCreateManifestFile(vfsMode: VFSMode) {
        vfsMode.withManifest { manifest ->
            expectThat(manifest) {
                get { this.getOperationCount() }.isEqualTo(0)
                get { this.getManifest() }.and {
                    get { this.lastAppliedOperationSequenceNumber }.isEqualTo(0)
                    get { this.stores }.isEmpty()
                }
            }
        }
    }

    @VirtualFileSystemTest
    fun canAddOperationsToManifest(vfsMode: VFSMode) {
        vfsMode.withManifest { manifest ->
            manifest.appendOperation { createStore(it, "foo/bar", compactionStrategy = LeveledCompactionStrategy()) }
            manifest.appendOperation { createStore(it, "foo/baz", compactionStrategy = TieredCompactionStrategy()) }
            manifest.appendOperation { flush(it, "foo/bar", 0) }
            manifest.appendOperation { flush(it, "foo/bar", 1) }
            manifest.appendOperation { flush(it, "foo/bar", 2) }
            manifest.appendOperation { flush(it, "foo/baz", 0) }
            manifest.appendOperation { flush(it, "foo/baz", 1) }
            manifest.appendOperation {
                leveledCompaction(
                    sequenceNumber = it,
                    storeId = "foo/bar",
                    lowerLevelIndex = 0,
                    lowerLevelFileIndices = setOf(0, 1, 2),
                    upperLevelIndex = 1,
                    upperLevelFileIndices = emptySet(),
                    outputFileIndices = setOf(3),
                    outputLevelIndex = 1,
                )
            }
            manifest.appendOperation {
                tieredCompaction(
                    sequenceNumber = it,
                    storeId = "foo/baz",
                    tierToFileIndices = mapOf(
                        0 to setOf(1),
                        1 to setOf(0),
                    ),
                    outputFileIndices = setOf(2),
                )
            }

            manifest.file.withInputStream { inputStream ->
                println(inputStream.bufferedReader().readText())
            }

            expectBeforeAndAfterReplay(manifest) {
                get { this.lastAppliedOperationSequenceNumber }.isEqualTo(9)
                get { this.stores }.hasSize(2).and {
                    get(StoreId.of("foo/bar")).isNotNull().and {
                        get { this.storeId }.isEqualTo(StoreId.of("foo/bar"))
                        get { this.lsmFiles }.hasSize(1).get(3).isNotNull().and {
                            get { this.fileIndex }.isEqualTo(3)
                            get { this.levelOrTier }.isEqualTo(1)
                        }
                    }
                    get(StoreId.of("foo/baz")).isNotNull().and {
                        get { this.storeId }.isEqualTo(StoreId.of("foo/baz"))
                        get { this.lsmFiles }.hasSize(1).and {
                            get(1).isNotNull().and {
                                get { this.fileIndex }.isEqualTo(2)
                                get { this.levelOrTier }.isEqualTo(1)
                            }
                        }
                    }
                }
            }
        }
    }

    // =================================================================================================================
    // TEST HELPER METHODS
    // =================================================================================================================

    /**
     * Creates the virtual file system and a "manifest.csm" [ManifestFile] file in it, then calls the given [action] on it.
     *
     * When this method returns, the virtual file system will be closed and the manifest file will be deleted!
     *
     * @param action The action to execute on the manifest file.
     *
     * @return The result of the action.
     */
    private fun <T> VFSMode.withManifest(action: (ManifestFile) -> T): T {
        return this.withVFS { vfs ->
            val manifest = ManifestFile(vfs.file("manifest.csm"))
            action(manifest)
        }
    }


    private fun createStore(
        sequenceNumber: Int,
        storeId: String,
        validFromTSN: TSN = 0,
        validToTSN: TSN? = null,
        createdByTransactionId: TransactionId = TransactionId.randomUUID(),
        lsmFiles: PMap<FileIndex, LSMFileInfo> = TreePMap.empty(),
        compactionStrategy: CompactionStrategy = CompactionStrategy.DEFAULT,
        wallClockTime: Timestamp = System.currentTimeMillis(),
    ): CreateStoreOperation {
        return CreateStoreOperation(
            sequenceNumber = sequenceNumber,
            storeMetadata = StoreMetadata(
                info = StoreInfo(
                    storeId = StoreId.of(storeId),
                    validFromTSN = validFromTSN,
                    validToTSN = validToTSN,
                    createdByTransactionId = createdByTransactionId,
                ),
                lsmFiles = lsmFiles,
                compactionStrategy = compactionStrategy,
            ),
            wallClockTime = wallClockTime,
        )
    }

    private fun flush(
        sequenceNumber: Int,
        storeId: String,
        fileIndex: Int,
        wallClockTime: Timestamp = System.currentTimeMillis(),
    ): FlushOperation {
        return FlushOperation(
            sequenceNumber = sequenceNumber,
            storeId = StoreId.of(storeId),
            fileIndex = fileIndex,
            wallClockTime = wallClockTime,
        )
    }

    private fun leveledCompaction(
        sequenceNumber: Int,
        storeId: String,
        lowerLevelIndex: LevelIndex,
        lowerLevelFileIndices: Set<FileIndex>,
        upperLevelIndex: LevelIndex,
        upperLevelFileIndices: Set<FileIndex>,
        outputFileIndices: Set<FileIndex>,
        outputLevelIndex: LevelIndex,
    ): LeveledCompactionOperation {
        return LeveledCompactionOperation(
            sequenceNumber = sequenceNumber,
            storeId = StoreId.of(storeId),
            lowerLevelIndex = lowerLevelIndex,
            lowerLevelFileIndices = lowerLevelFileIndices,
            upperLevelIndex = upperLevelIndex,
            upperLevelFileIndices = upperLevelFileIndices,
            outputFileIndices = outputFileIndices,
            outputLevelIndex = outputLevelIndex
        )
    }

    private fun tieredCompaction(
        sequenceNumber: Int,
        storeId: String,
        tierToFileIndices: Map<TierIndex, Set<FileIndex>>,
        outputFileIndices: Set<FileIndex>,
        wallClockTime: Timestamp = System.currentTimeMillis(),
    ): TieredCompactionOperation {
        return TieredCompactionOperation(
            sequenceNumber = sequenceNumber,
            storeId = StoreId.of(storeId),
            tierToFileIndices = tierToFileIndices,
            outputFileIndices = outputFileIndices,
            wallClockTime = wallClockTime,
        )
    }

    private fun expectBeforeAndAfterReplay(
        manifestFile: ManifestFile,
        assertion: Assertion.Builder<Manifest>.() -> Unit,
    ) {
        expectThat(manifestFile.getManifest(), assertion)

        val manifestReplayed = ManifestFile(manifestFile.file)
        expectThat(manifestReplayed.getManifest(), assertion)
    }

}