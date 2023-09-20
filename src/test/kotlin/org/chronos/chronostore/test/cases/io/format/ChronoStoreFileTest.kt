package org.chronos.chronostore.test.cases.io.format

import org.chronos.chronostore.io.fileaccess.FileChannelDriver
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.io.fileaccess.MemorySegmentFileDriver
import org.chronos.chronostore.io.format.*
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.chronos.chronostore.lsm.cache.LocalBlockCache
import org.chronos.chronostore.test.util.VFSMode
import org.chronos.chronostore.test.util.VirtualFileSystemTest
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.unit.KiB
import org.chronos.chronostore.util.unit.MiB
import strikt.api.expectThat
import strikt.assertions.*
import kotlin.random.Random

class ChronoStoreFileTest {

    @VirtualFileSystemTest
    fun canCreateAndReadEmptyFile(mode: VFSMode) {
        mode.withVFS { vfs ->
            val file = vfs.file("test.chronostore")

            file.withOverWriter { overWriter ->
                val writer = ChronoStoreFileWriter(
                    outputStream = overWriter.outputStream.buffered(),
                    settings = ChronoStoreFileSettings(CompressionAlgorithm.NONE, 16.MiB, 100),
                    metadata = emptyMap()
                )
                writer.writeFile(0, orderedCommands = emptySequence<Command>().iterator())
                overWriter.commit()
            }

            expectThat(file) {
                get { this.exists() }.isTrue()
                get { this.length }.isGreaterThan(0L)
            }

            val factory = MemorySegmentFileDriver.Factory
            factory.createDriver(file).use { driver ->
                ChronoStoreFileReader(driver, LocalBlockCache.NONE).use { reader ->
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
                                get { this.totalEntries }.isEqualTo(0)
                                get { this.headHistoryRatio }.isEqualTo(1.0)
                                get { this.createdAt }.isGreaterThan(0L)
                                get { this.settings }.and {
                                    get { this.compression }.isEqualTo(CompressionAlgorithm.NONE)
                                    get { this.maxBlockSize }.isEqualTo(16.MiB)
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


    @VirtualFileSystemTest
    fun canCreateAndReadFileWith1000VersionsOfSameKey(mode: VFSMode) {
        mode.withVFS { vfs ->
            val file = vfs.file("test.chronostore")

            val theKey = Bytes("theKey")
            file.withOverWriter { overWriter ->
                val writer = ChronoStoreFileWriter(
                    outputStream = overWriter.outputStream.buffered(),
                    settings = ChronoStoreFileSettings(CompressionAlgorithm.NONE, 16.KiB, 4),
                    metadata = emptyMap()
                )
                val random = Random(System.currentTimeMillis())
                val commands = (0 until 1000).asSequence().map { i ->
                    if (i.mod(100) == 0 && i > 0) {
                        Command.del(theKey, (i + 1) * 1000L)
                    } else {
                        Command.put(theKey, (i + 1) * 1000L, Bytes.random(random, 1024))
                    }
                }
                writer.writeFile(0, orderedCommands = commands.iterator())
                overWriter.commit()
            }

            expectThat(file) {
                get { exists() }.isTrue()
                get { length }.isGreaterThan(0L)
            }

            val factory = FileChannelDriver.Factory
            factory.createDriver(file).use { driver ->
                ChronoStoreFileReader(driver, LocalBlockCache.NONE).use { reader ->
                    val min = reader.fileHeader.metaData.minTimestamp!!
                    val max = reader.fileHeader.metaData.maxTimestamp!!

                    expectThat(reader) {
                        get { fileHeader.indexOfBlocks.size }.isGreaterThan(1)

                        get { get(KeyAndTimestamp(theKey, max + 1)) }.isNotNull().and {
                            get { key }.isEqualTo(theKey)
                            get { timestamp }.isEqualTo(max)
                            get { opCode }.isEqualTo(Command.OpCode.PUT)
                            get { value }.hasSize(1024)
                        }

                        get { get(KeyAndTimestamp(theKey, min - 1)) }.isNull()

                        get { get(KeyAndTimestamp(theKey, 101_000)) }.isNotNull().and {
                            get { opCode }.isEqualTo(Command.OpCode.DEL)
                            get { timestamp }.isEqualTo(101_000)
                            get { value }.isEmpty()
                        }
                        get { get(KeyAndTimestamp(theKey, 101_001)) }.isNotNull().and {
                            get { opCode }.isEqualTo(Command.OpCode.DEL)
                            get { timestamp }.isEqualTo(101_000)
                            get { value }.isEmpty()
                        }
                        get { get(KeyAndTimestamp(theKey, 101_002)) }.isNotNull().and {
                            get { opCode }.isEqualTo(Command.OpCode.DEL)
                            get { timestamp }.isEqualTo(101_000)
                            get { value }.isEmpty()
                        }
                        get { get(KeyAndTimestamp(theKey, 101_010)) }.isNotNull().and {
                            get { opCode }.isEqualTo(Command.OpCode.DEL)
                            get { timestamp }.isEqualTo(101_000)
                            get { value }.isEmpty()
                        }
                        get { get(KeyAndTimestamp(theKey, 101_100)) }.isNotNull().and {
                            get { opCode }.isEqualTo(Command.OpCode.DEL)
                            get { timestamp }.isEqualTo(101_000)
                            get { value }.isEmpty()
                        }
                        get { get(KeyAndTimestamp(theKey, 101_990)) }.isNotNull().and {
                            get { opCode }.isEqualTo(Command.OpCode.DEL)
                            get { timestamp }.isEqualTo(101_000)
                            get { value }.isEmpty()
                        }
                        get { get(KeyAndTimestamp(theKey, 101_999)) }.isNotNull().and {
                            get { opCode }.isEqualTo(Command.OpCode.DEL)
                            get { timestamp }.isEqualTo(101_000)
                            get { value }.isEmpty()
                        }
                        get { get(KeyAndTimestamp(theKey, 102_000)) }.isNotNull().and {
                            get { opCode }.isEqualTo(Command.OpCode.PUT)
                            get { timestamp }.isEqualTo(102_000)
                            get { value }.hasSize(1024)
                        }
                        get { get(KeyAndTimestamp(theKey, 102_001)) }.isNotNull().and {
                            get { opCode }.isEqualTo(Command.OpCode.PUT)
                            get { timestamp }.isEqualTo(102_000)
                            get { value }.hasSize(1024)
                        }
                    }
                }
            }
        }
    }

    @VirtualFileSystemTest
    fun canIterateOverFileWith1000VersionsOfSameKeyWithCursor(mode: VFSMode) {
        mode.withVFS { vfs ->
            val file = vfs.file("test.chronostore")

            val theKey = Bytes("theKey")
            file.withOverWriter { overWriter ->
                val writer = ChronoStoreFileWriter(
                    outputStream = overWriter.outputStream.buffered(),
                    settings = ChronoStoreFileSettings(CompressionAlgorithm.NONE, 16.KiB, 4),
                    metadata = emptyMap()
                )
                val random = Random(System.currentTimeMillis())
                val commands = (0 until 1000).asSequence().map { i ->
                    if (i.mod(100) == 0 && i > 0) {
                        Command.del(theKey, (i + 1) * 1000L)
                    } else {
                        Command.put(theKey, (i + 1) * 1000L, Bytes.random(random, 1024))
                    }
                }
                writer.writeFile(0, orderedCommands = commands.iterator())
                overWriter.commit()
            }

            expectThat(file) {
                get { this.exists() }.isTrue()
                get { this.length }.isGreaterThan(0L)
            }

            val factory = FileChannelDriver.Factory
            factory.createDriver(file).use { driver ->
                ChronoStoreFileReader(driver, LocalBlockCache.NONE).use { reader ->
                    reader.openCursor().use { cursor ->
                        assert(!cursor.isValidPosition) { "cursor.isValidPosition returned TRUE after initialization!" }
                        assert(cursor.first()) { "cursor.first returned FALSE!" }
                        assert(cursor.isValidPosition) { "cursor.isValidPosition returned FALSE after first()!" }
                        assert(!cursor.previous()) { "could navigate to previous() from first()!" }
                        val entries = cursor.ascendingEntrySequenceFromHere().toList()
                        expectThat(entries).hasSize(1000)

                    }
                }
            }
        }
    }

}