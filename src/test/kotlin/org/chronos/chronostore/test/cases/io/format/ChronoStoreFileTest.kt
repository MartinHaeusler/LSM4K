package org.chronos.chronostore.test.cases.io.format

import org.chronos.chronostore.command.KeyAndTimestamp
import org.chronos.chronostore.io.fileaccess.MemorySegmentFileDriver
import org.chronos.chronostore.io.format.*
import org.chronos.chronostore.io.format.datablock.BlockReadMode
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.chronos.chronostore.test.util.VFSMode
import org.chronos.chronostore.test.util.VirtualFileSystemTest
import org.chronos.chronostore.util.Bytes
import strikt.api.expectThat
import strikt.assertions.*

class ChronoStoreFileTest {

    @VirtualFileSystemTest
    fun canCreateAndReadEmptyFile(mode: VFSMode) {
        mode.withVFS { vfs ->
            val file = vfs.file("test.chronostore")

            file.withOverWriter { overWriter ->
                val writer = ChronoStoreFileWriter(
                    outputStream = overWriter.outputStream.buffered(),
                    settings = ChronoStoreFileSettings(CompressionAlgorithm.NONE, 1024 * 1024 * 16, 100),
                    metadata = emptyMap()
                )
                writer.writeFile(orderedCommands = emptySequence())
                overWriter.commit()
            }

            expectThat(file) {
                get { this.exists() }.isTrue()
                get { this.length }.isGreaterThan(0L)
            }

            val factory = MemorySegmentFileDriver.Factory()
            factory.createDriver(file).use { driver ->
                ChronoStoreFileReader(driver, BlockReadMode.DISK_BASED).use { reader ->
                    expectThat(reader) {
                        get { fileHeader }.and {
                            get { this.indexOfBlocks.isEmpty }.isTrue()
                            get { this.fileFormatVersion }.isEqualTo(ChronoStoreFileFormat.Version.V_1_0_0)
                            get { this.metaData }.and {
                                get { this.minKey }.isNull()
                                get { this.maxKey }.isNull()
                                get { this.minTimestamp }.isNull()
                                get { this.maxTimestamp }.isNull()
                                get { this.infoMap }.isEmpty()
                                get { this.headEntries }.isEqualTo(0L)
                                get { this.historyEntries }.isEqualTo(0L)
                                get { this.totalEntries}.isEqualTo(0)
                                get { this.headHistoryRatio }.isEqualTo(1.0)
                                get { this.createdAt }.isGreaterThan(0L)
                                get { this.settings }.and {
                                    get { this.compression }.isEqualTo(CompressionAlgorithm.NONE)
                                    get { this.maxBlockSizeInBytes }.isEqualTo(1024 * 1024 * 16)
                                    get { this.indexRate }.isEqualTo(100)
                                }
                            }
                            get { this.trailer }.and {
                                get { this.beginOfBlocks }.isEqualTo(ChronoStoreFileFormat.FILE_MAGIC_BYTES.size.toLong() + Int.SIZE_BYTES /* format version */)
                                get { this.beginOfMetadata == this.beginOfIndexOfBlocks }.isTrue()
                                get { this.beginOfIndexOfBlocks == this.beginOfMetadata }.isTrue()
                            }
                        }
                        get { this.get(KeyAndTimestamp(Bytes("bullshit"), 1234)) }.isNull()
                    }
                }
            }
        }
    }

}