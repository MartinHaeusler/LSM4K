package org.lsm4k.test.cases.io.format

import org.lsm4k.compressor.NoCompressor
import org.lsm4k.io.fileaccess.FileChannelDriver
import org.lsm4k.io.fileaccess.MemorySegmentFileDriver
import org.lsm4k.io.format.*
import org.lsm4k.io.format.LSMFileFormat.getLatestVersion
import org.lsm4k.io.format.cursor.LSMFileCursor
import org.lsm4k.io.format.writer.StandardLSMStoreFileWriter
import org.lsm4k.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.lsm4k.model.command.Command
import org.lsm4k.model.command.KeyAndTSN
import org.lsm4k.model.command.OpCode
import org.lsm4k.test.util.VFSMode
import org.lsm4k.test.util.VirtualFileSystemTest
import org.lsm4k.util.bytes.BasicBytes
import org.lsm4k.util.bytes.Bytes
import org.lsm4k.util.statistics.StatisticsCollector
import org.lsm4k.util.unit.BinarySize.Companion.KiB
import org.lsm4k.util.unit.BinarySize.Companion.MiB
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.*
import kotlin.random.Random

class LSMFileTest {

    @VirtualFileSystemTest
    fun canCreateAndReadEmptyFile(mode: VFSMode) {
        val stats = StatisticsCollector()
        mode.withVFS { vfs ->
            val file = vfs.file("test.lsm")

            file.withOverWriter { overWriter ->
                val writer = StandardLSMStoreFileWriter(
                    outputStream = overWriter.outputStream,
                    settings = LSMFileSettings(CompressionAlgorithm(NoCompressor()), 16.MiB),
                    statisticsReporter = stats,
                )
                writer.write(
                    numberOfMerges = 0,
                    orderedCommands = emptySequence<Command>().iterator(),
                    commandCountEstimate = 10,
                    maxCompletelyWrittenTSN = 1,
                )
                overWriter.commit()
            }

            expectThat(file) {
                get { this.exists() }.isTrue()
                get { this.length }.isGreaterThan(0L)
            }

            val factory = MemorySegmentFileDriver.Factory
            val blockLoader = BlockLoader.basic(factory, stats)

            factory.createDriver(file).use { driver ->
                val fileHeader = LSMFileFormat.loadFileHeader(driver)
                val latestVersion = getLatestVersion(
                    file = file,
                    fileHeader = fileHeader,
                    keyAndTSN = KeyAndTSN(BasicBytes("bullshit"), 1234),
                    blockLoader = blockLoader,
                )
                expect {
                    that(fileHeader) {
                        get { this.indexOfBlocks.isEmpty }.isTrue()
                        get { this.fileFormatVersion }.isEqualTo(FileFormatVersion.V_1_0_0)
                        get { this.metaData }.and {
                            get { this.minKey }.isNull()
                            get { this.maxKey }.isNull()
                            get { this.minTSN }.isNull()
                            get { this.maxTSN }.isNull()
                            get { this.maxCompletelyWrittenTSN }.isEqualTo(1L)
                            get { this.headEntries }.isEqualTo(0L)
                            get { this.historyEntries }.isEqualTo(0L)
                            get { this.totalEntries }.isEqualTo(0)
                            get { this.headHistoryRatio }.isEqualTo(1.0)
                            get { this.createdAt }.isGreaterThan(0L)
                            get { this.settings }.and {
                                get { this.compression }.isEqualTo(CompressionAlgorithm(NoCompressor()))
                                get { this.maxBlockSize }.isEqualTo(16.MiB)
                            }
                        }
                        get { this.trailer }.and {
                            get { this.beginOfBlocks }.isEqualTo(LSMFileFormat.FILE_MAGIC_BYTES.size.toLong() + Int.SIZE_BYTES /* format version */)
                            get { this.beginOfMetadata == this.beginOfIndexOfBlocks }.isTrue()
                            get { this.beginOfIndexOfBlocks == this.beginOfMetadata }.isTrue()
                        }
                    }
                    that(latestVersion).isNull()
                }
            }
        }
    }

    @VirtualFileSystemTest
    fun canCreateAndReadFileWith1Key(mode: VFSMode) {
        val stats = StatisticsCollector()
        mode.withVFS { vfs ->

            val file = vfs.file("test.lsm")

            val theKey = BasicBytes("theKey")
            file.withOverWriter { overWriter ->
                val writer = StandardLSMStoreFileWriter(
                    outputStream = overWriter.outputStream.buffered(),
                    settings = LSMFileSettings(CompressionAlgorithm(NoCompressor()), 16.KiB),
                    statisticsReporter = stats,
                )
                val commands = listOf(Command.put(theKey, 1000L, BasicBytes("hello")))
                writer.write(
                    numberOfMerges = 0,
                    orderedCommands = commands.iterator(),
                    commandCountEstimate = 10,
                    maxCompletelyWrittenTSN = 42,
                )
                overWriter.commit()
            }

            expectThat(file) {
                get { exists() }.isTrue()
                get { length }.isGreaterThan(0L)
            }

            val factory = FileChannelDriver.Factory
            val blockLoader = BlockLoader.basic(factory, stats)

            factory.createDriver(file).use { driver ->
                val header = LSMFileFormat.loadFileHeader(driver)

                val min = header.metaData.minTSN!!
                val max = header.metaData.maxTSN!!

                expect {
                    that(header) {
                        get { indexOfBlocks.size }.isEqualTo(1)
                        get { metaData.maxCompletelyWrittenTSN }.isEqualTo(42L)
                    }
                    that(getLatestVersion(file, header, KeyAndTSN(theKey, max + 1), blockLoader)).isNotNull().and {
                        get { key }.isEqualTo(theKey)
                        get { tsn }.isEqualTo(max)
                        get { opCode }.isEqualTo(OpCode.PUT)
                        get { value.asString() }.isEqualTo("hello")
                    }
                    that(getLatestVersion(file, header, KeyAndTSN(theKey, min - 1), blockLoader)).isNull()
                    that(getLatestVersion(file, header, KeyAndTSN(theKey, 1000), blockLoader)).isNotNull().and {
                        get { opCode }.isEqualTo(OpCode.PUT)
                        get { tsn }.isEqualTo(1000)
                        get { value.asString() }.isEqualTo("hello")
                    }
                }
            }
        }
    }

    @VirtualFileSystemTest
    fun canCreateAndReadFileWith1000VersionsOfSameKey(mode: VFSMode) {
        val stats = StatisticsCollector()

        mode.withVFS { vfs ->
            val file = vfs.file("test.lsm")

            val theKey = BasicBytes("theKey")
            file.withOverWriter { overWriter ->
                val writer = StandardLSMStoreFileWriter(
                    outputStream = overWriter.outputStream.buffered(),
                    settings = LSMFileSettings(CompressionAlgorithm(NoCompressor()), 16.KiB),
                    statisticsReporter = stats,
                )
                val random = Random(System.currentTimeMillis())
                val commands = (0..<1000).asSequence().map { i ->
                    if (i.mod(100) == 0 && i > 0) {
                        Command.del(theKey, (i + 1) * 1000L)
                    } else {
                        Command.put(theKey, (i + 1) * 1000L, Bytes.random(random, 1024))
                    }
                }
                writer.write(
                    numberOfMerges = 0,
                    orderedCommands = commands.iterator(),
                    commandCountEstimate = 1000,
                    maxCompletelyWrittenTSN = 34233,
                )
                overWriter.commit()
            }

            expectThat(file) {
                get { exists() }.isTrue()
                get { length }.isGreaterThan(0L)
            }

            val factory = FileChannelDriver.Factory
            val blockLoader = BlockLoader.basic(factory, stats)

            factory.createDriver(file).use { driver ->
                val header = LSMFileFormat.loadFileHeader(driver)
                val min = header.metaData.minTSN!!
                val max = header.metaData.maxTSN!!

                expect {
                    that(header) {
                        get { this.indexOfBlocks.size }.isGreaterThan(1)
                        get { this.metaData.maxCompletelyWrittenTSN }.isEqualTo(34233L)
                    }
                    that(getLatestVersion(file, header, KeyAndTSN(theKey, max + 1), blockLoader)).isNotNull().and {
                        get { this.key }.isEqualTo(theKey)
                        get { this.tsn }.isEqualTo(max)
                        get { this.opCode }.isEqualTo(OpCode.PUT)
                        get { this.value }.hasSize(1024)
                    }

                    that(getLatestVersion(file, header, KeyAndTSN(theKey, min - 1), blockLoader)).isNull()

                    that(getLatestVersion(file, header, KeyAndTSN(theKey, 101_000), blockLoader)).isNotNull().and {
                        get { this.opCode }.isEqualTo(OpCode.DEL)
                        get { this.tsn }.isEqualTo(101_000)
                        get { this.value }.isEmpty()
                    }
                    that(getLatestVersion(file, header, KeyAndTSN(theKey, 101_001), blockLoader)).isNotNull().and {
                        get { this.opCode }.isEqualTo(OpCode.DEL)
                        get { this.tsn }.isEqualTo(101_000)
                        get { this.value }.isEmpty()
                    }
                    that(getLatestVersion(file, header, KeyAndTSN(theKey, 101_002), blockLoader)).isNotNull().and {
                        get { this.opCode }.isEqualTo(OpCode.DEL)
                        get { this.tsn }.isEqualTo(101_000)
                        get { this.value }.isEmpty()
                    }
                    that(getLatestVersion(file, header, KeyAndTSN(theKey, 101_010), blockLoader)).isNotNull().and {
                        get { this.opCode }.isEqualTo(OpCode.DEL)
                        get { this.tsn }.isEqualTo(101_000)
                        get { this.value }.isEmpty()
                    }
                    that(getLatestVersion(file, header, KeyAndTSN(theKey, 101_100), blockLoader)).isNotNull().and {
                        get { this.opCode }.isEqualTo(OpCode.DEL)
                        get { this.tsn }.isEqualTo(101_000)
                        get { this.value }.isEmpty()
                    }
                    that(getLatestVersion(file, header, KeyAndTSN(theKey, 101_990), blockLoader)).isNotNull().and {
                        get { this.opCode }.isEqualTo(OpCode.DEL)
                        get { this.tsn }.isEqualTo(101_000)
                        get { this.value }.isEmpty()
                    }
                    that(getLatestVersion(file, header, KeyAndTSN(theKey, 101_999), blockLoader)).isNotNull().and {
                        get { this.opCode }.isEqualTo(OpCode.DEL)
                        get { this.tsn }.isEqualTo(101_000)
                        get { this.value }.isEmpty()
                    }
                    that(getLatestVersion(file, header, KeyAndTSN(theKey, 102_000), blockLoader)).isNotNull().and {
                        get { this.opCode }.isEqualTo(OpCode.PUT)
                        get { this.tsn }.isEqualTo(102_000)
                        get { this.value }.hasSize(1024)
                    }
                    that(getLatestVersion(file, header, KeyAndTSN(theKey, 102_001), blockLoader)).isNotNull().and {
                        get { this.opCode }.isEqualTo(OpCode.PUT)
                        get { this.tsn }.isEqualTo(102_000)
                        get { this.value }.hasSize(1024)
                    }
                }
            }
        }
    }

    @VirtualFileSystemTest
    fun canIterateOverFileWith1000VersionsOfSameKeyWithCursor(mode: VFSMode) {
        val stats = StatisticsCollector()
        mode.withVFS { vfs ->
            val file = vfs.file("test.lsm")

            val theKey = BasicBytes("theKey")
            file.withOverWriter { overWriter ->
                val writer = StandardLSMStoreFileWriter(
                    outputStream = overWriter.outputStream.buffered(),
                    settings = LSMFileSettings(CompressionAlgorithm(NoCompressor()), 16.KiB),
                    statisticsReporter = stats,
                )
                val random = Random(System.currentTimeMillis())
                val commands = (0..<1000).asSequence().map { i ->
                    if (i.mod(100) == 0 && i > 0) {
                        Command.del(theKey, (i + 1) * 1000L)
                    } else {
                        Command.put(theKey, (i + 1) * 1000L, Bytes.random(random, 1024))
                    }
                }
                writer.write(
                    numberOfMerges = 0,
                    orderedCommands = commands.iterator(),
                    commandCountEstimate = 1000,
                    maxCompletelyWrittenTSN = null,
                )
                overWriter.commit()
            }

            expectThat(file) {
                get { this.exists() }.isTrue()
                get { this.length }.isGreaterThan(0L)
            }

            val factory = FileChannelDriver.Factory
            val blockLoader = BlockLoader.basic(factory, stats)

            val header = factory.createDriver(file).use { driver ->
                LSMFileFormat.loadFileHeader(driver)
            }

            LSMFileCursor(file, header, blockLoader).use { cursor ->
                assert(!cursor.isValidPosition) { "cursor.isValidPosition returned TRUE after initialization!" }
                assert(cursor.first()) { "cursor.first returned FALSE!" }
                assert(cursor.isValidPosition) { "cursor.isValidPosition returned FALSE after first()!" }
                assert(!cursor.previous()) { "could navigate to previous() from first()!" }
                val entries = cursor.ascendingEntrySequenceFromHere().toList()
                expectThat(entries).hasSize(1000)
            }
        }
    }

    @VirtualFileSystemTest
    fun canCreateAndReadFileWithFixedEntries(mode: VFSMode) {
        val stats = StatisticsCollector()
        mode.withVFS { vfs ->
            val file = vfs.file("test.lsm")

            val beginOfTest = System.currentTimeMillis()

            val commands = listOf(
                Command.put("foo", 10_000, "bar"),
                Command.put("foo", 100_000, "baz"),
                Command.put("hello", 1234, "world"),
                Command.put("hello", 1235, "foo"),
                Command.put(BasicBytes("hello"), 1240, Bytes.EMPTY),
            )

            file.withOverWriter { overWriter ->
                StandardLSMStoreFileWriter(
                    outputStream = overWriter.outputStream.buffered(),
                    settings = LSMFileSettings(CompressionAlgorithm.forCompressorName("snappy"), 16.KiB),
                    statisticsReporter = stats,
                ).use { writer ->
                    writer.write(
                        numberOfMerges = 0L,
                        orderedCommands = commands.iterator(),
                        commandCountEstimate = 10,
                        maxCompletelyWrittenTSN = 673456434L,
                    )
                }
                overWriter.commit()
            }

            val driverFactory = FileChannelDriver.Factory
            val blockLoader = BlockLoader.basic(driverFactory, stats)

            driverFactory.createDriver(file).use { driver ->
                val header = LSMFileFormat.loadFileHeader(driver)
                expectThat(header) {
                    get { this.indexOfBlocks }.and {
                        get { this.size }.isEqualTo(1)
                        get { this.isEmpty }.isFalse()
                        get { this.isValidBlockIndex(0) }.isTrue()
                        get { this.isValidBlockIndex(1) }.isFalse()
                        get { this.isValidBlockIndex(-1) }.isFalse()
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("hello"), 1237)) }.isEqualTo(0)
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("hello"), 1240)) }.isEqualTo(0)
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("hello"), 1241)) }.isNull()
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("hello"), 0)) }.isEqualTo(0)
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("foo"), 0)) }.isEqualTo(0)
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("foo"), 9_999)) }.isEqualTo(0)
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("foo"), 10_000)) }.isEqualTo(0)
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("foo"), 200_000)) }.isEqualTo(0)
                    }
                    get { this.trailer }.and {
                        get { this.beginOfBlocks }.isGreaterThan(0L)
                        get { this.beginOfBlocks }.isLessThan(header.trailer.beginOfIndexOfBlocks)
                        get { this.beginOfIndexOfBlocks }.isLessThan(header.trailer.beginOfMetadata)
                    }
                    get { this.fileFormatVersion }.isGreaterThanOrEqualTo(FileFormatVersion.V_1_0_0)
                    get { this.metaData }.and {
                        get { this.createdAt }.isGreaterThanOrEqualTo(beginOfTest).isLessThanOrEqualTo(System.currentTimeMillis())
                        get { this.totalEntries }.isEqualTo(5)
                        get { this.headEntries }.isEqualTo(2)
                        get { this.historyEntries }.isEqualTo(3)
                        get { this.headHistoryRatio }.isEqualTo(0.4)
                        get { this.numberOfMerges }.isEqualTo(0)
                        get { this.numberOfBlocks }.isEqualTo(1)
                        get { this.minKey }.isNotNull().get { this.asString() }.isEqualTo("foo")
                        get { this.maxKey }.isNotNull().get { this.asString() }.isEqualTo("hello")
                        get { this.minTSN }.isEqualTo(1234)
                        get { this.maxTSN }.isEqualTo(100_000)
                        get { this.maxCompletelyWrittenTSN }.isEqualTo(673456434L)
                        get { this.settings }.and {
                            get { this.maxBlockSize }.isEqualTo(16.KiB)
                            get { this.compression }.isEqualTo(CompressionAlgorithm.forCompressorName("snappy"))
                        }
                    }
                }

                val entries = LSMFileCursor(file, header, blockLoader).use { cursor ->
                    cursor.firstOrThrow()
                    cursor.ascendingEntrySequenceFromHere().toList()
                }

                expectThat(entries).containsExactly(
                    KeyAndTSN(BasicBytes("foo"), 10_000) to Command.put("foo", 10_000, "bar"),
                    KeyAndTSN(BasicBytes("foo"), 100_000) to Command.put("foo", 100_000, "baz"),
                    KeyAndTSN(BasicBytes("hello"), 1234) to Command.put("hello", 1234, "world"),
                    KeyAndTSN(BasicBytes("hello"), 1235) to Command.put("hello", 1235, "foo"),
                    KeyAndTSN(BasicBytes("hello"), 1240) to Command.put(BasicBytes("hello"), 1240, Bytes.EMPTY),
                )
            }
        }
    }

    @VirtualFileSystemTest
    fun canCreateAndReadFileWithFixedEntriesAfterMerge(mode: VFSMode) {
        val stats = StatisticsCollector()
        mode.withVFS { vfs ->
            val file = vfs.file("test.lsm")

            val beginOfTest = System.currentTimeMillis()

            val commands = listOf(
                Command.put("foo", 10_000, "bar"),
                Command.put("foo", 100_000, "baz"),
                Command.put("hello", 1234, "world"),
                Command.put("hello", 1235, "foo"),
                Command.put(BasicBytes("hello"), 1240, Bytes.EMPTY),
            )

            file.withOverWriter { overWriter ->
                StandardLSMStoreFileWriter(
                    outputStream = overWriter.outputStream.buffered(),
                    settings = LSMFileSettings(CompressionAlgorithm.forCompressorName("snappy"), 16.KiB),
                    statisticsReporter = stats,
                ).use { writer ->
                    writer.write(
                        numberOfMerges = 0L,
                        orderedCommands = commands.iterator(),
                        commandCountEstimate = 10,
                        maxCompletelyWrittenTSN = null,
                    )
                }
                overWriter.commit()
            }

            val emptyFile = vfs.file("test2.lsm")
            emptyFile.withOverWriter { overWriter ->
                val writer = StandardLSMStoreFileWriter(
                    outputStream = overWriter.outputStream.buffered(),
                    settings = LSMFileSettings(CompressionAlgorithm.forCompressorName("snappy"), 16.MiB),
                    statisticsReporter = stats,
                )
                writer.write(
                    numberOfMerges = 0,
                    orderedCommands = emptySequence<Command>().iterator(),
                    commandCountEstimate = 10,
                    maxCompletelyWrittenTSN = null,
                )
                overWriter.commit()
            }

            val driverFactory = FileChannelDriver.Factory
            val blockLoader = BlockLoader.basic(driverFactory, stats)

            driverFactory.createDriver(file).use { driver ->
                val header = LSMFileFormat.loadFileHeader(driver)
                expectThat(header) {
                    get { this.indexOfBlocks }.and {
                        get { this.size }.isEqualTo(1)
                        get { this.isEmpty }.isFalse()
                        get { this.isValidBlockIndex(0) }.isTrue()
                        get { this.isValidBlockIndex(1) }.isFalse()
                        get { this.isValidBlockIndex(-1) }.isFalse()
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("hello"), 1237)) }.isEqualTo(0)
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("hello"), 1240)) }.isEqualTo(0)
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("hello"), 1241)) }.isNull()
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("hello"), 0)) }.isEqualTo(0)
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("foo"), 0)) }.isEqualTo(0)
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("foo"), 9_999)) }.isEqualTo(0)
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("foo"), 10_000)) }.isEqualTo(0)
                        get { this.getBlockIndexForKeyAndTimestampAscending(KeyAndTSN(BasicBytes("foo"), 200_000)) }.isEqualTo(0)
                    }
                    get { this.trailer }.and {
                        get { this.beginOfBlocks }.isGreaterThan(0L)
                        get { this.beginOfBlocks }.isLessThan(header.trailer.beginOfIndexOfBlocks)
                        get { this.beginOfIndexOfBlocks }.isLessThan(header.trailer.beginOfMetadata)
                    }
                    get { this.fileFormatVersion }.isGreaterThanOrEqualTo(FileFormatVersion.V_1_0_0)
                    get { this.metaData }.and {
                        get { this.createdAt }.isGreaterThanOrEqualTo(beginOfTest).isLessThanOrEqualTo(System.currentTimeMillis())
                        get { this.totalEntries }.isEqualTo(5)
                        get { this.headEntries }.isEqualTo(2)
                        get { this.historyEntries }.isEqualTo(3)
                        get { this.headHistoryRatio }.isEqualTo(0.4)
                        get { this.numberOfMerges }.isEqualTo(0)
                        get { this.numberOfBlocks }.isEqualTo(1)
                        get { this.minKey }.isNotNull().get { this.asString() }.isEqualTo("foo")
                        get { this.maxKey }.isNotNull().get { this.asString() }.isEqualTo("hello")
                        get { this.minTSN }.isEqualTo(1234)
                        get { this.maxTSN }.isEqualTo(100_000)
                        get { this.maxCompletelyWrittenTSN }.isNull()
                        get { this.settings }.and {
                            get { this.maxBlockSize }.isEqualTo(16.KiB)
                            get { this.compression }.isEqualTo(CompressionAlgorithm.forCompressorName("snappy"))
                        }
                    }
                }


                val entries = LSMFileCursor(file, header, blockLoader).use { cursor ->
                    cursor.firstOrThrow()
                    cursor.ascendingEntrySequenceFromHere().toList()
                }

                expectThat(entries).containsExactly(
                    KeyAndTSN(BasicBytes("foo"), 10_000) to Command.put("foo", 10_000, "bar"),
                    KeyAndTSN(BasicBytes("foo"), 100_000) to Command.put("foo", 100_000, "baz"),
                    KeyAndTSN(BasicBytes("hello"), 1234) to Command.put("hello", 1234, "world"),
                    KeyAndTSN(BasicBytes("hello"), 1235) to Command.put("hello", 1235, "foo"),
                    KeyAndTSN(BasicBytes("hello"), 1240) to Command.put(BasicBytes("hello"), 1240, Bytes.EMPTY),
                )
            }
        }
    }
}