package io.github.martinhaeusler.lsm4k.test.cases.wal

import io.github.martinhaeusler.lsm4k.api.exceptions.WriteAheadLogCorruptedException
import io.github.martinhaeusler.lsm4k.io.structure.LSM4KStructure
import io.github.martinhaeusler.lsm4k.io.structure.LSM4KStructure.WRITE_AHEAD_LOG_FILE_PREFIX
import io.github.martinhaeusler.lsm4k.io.structure.LSM4KStructure.WRITE_AHEAD_LOG_FILE_SUFFIX
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualReadWriteFile
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.test.util.VFSMode
import io.github.martinhaeusler.lsm4k.test.util.VirtualFileSystemTest
import io.github.martinhaeusler.lsm4k.util.IOExtensions.withInputStream
import io.github.martinhaeusler.lsm4k.util.LittleEndianExtensions.readLittleEndianLongOrNull
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.wal.WALReadBuffer
import io.github.martinhaeusler.lsm4k.wal.WriteAheadLog
import io.github.martinhaeusler.lsm4k.wal.format.TransactionCommandEntry
import io.github.martinhaeusler.lsm4k.wal.format.TransactionCommitEntry
import io.github.martinhaeusler.lsm4k.wal.format.TransactionStartEntry
import io.github.martinhaeusler.lsm4k.wal.format.WALEntry
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class WriteAheadLogTest {

    @VirtualFileSystemTest
    fun canWriteAndReadTransactions(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val wal = WriteAheadLog(vfs.directory("wal"))

            val store1Name = StoreId.of("d272b6e7-e1bc-4d6f-bf04-93e36445b279")
            val store2Name = StoreId.of("508e1b3c-3383-47c4-bdfe-13175c88a146")
            val store3Name = StoreId.of("c4071866-48fe-48df-af6b-5e29f1e833cb")

            val tx1 = FakeTransaction(
                commitTSN = 1000,
                storeIdToCommands = mapOf(
                    store1Name to listOf(
                        Command.put("hello", 1000, "world"),
                        Command.put("foo", 1000, "bar"),
                    ),
                    store2Name to listOf(
                        Command.put("pi", 1000, "3.1415"),
                        Command.put("e", 1000, "2.71828")
                    ),
                    store3Name to listOf(
                        Command.put("john", 1000, "doe"),
                        Command.put("jane", 1000, "doe"),
                    )
                ),
            )

            val tx2 = FakeTransaction(
                commitTSN = 1234,
                storeIdToCommands = emptyMap(),
            )


            val tx3 = FakeTransaction(
                commitTSN = 777777,
                storeIdToCommands = mapOf(
                    store1Name to listOf(
                        Command.del("hello", 777777),
                        Command.put("foo", 777777, "baz"),
                    ),
                    store3Name to listOf(
                        Command.put("sarah", 777777, "doe"),
                    )
                ),
            )

            wal.addCommittedTransaction(tx1)
            wal.addCommittedTransaction(tx2)
            wal.addCommittedTransaction(tx3)

            val walContent = wal.readContent()

            expectThat(walContent) {
                get { this.storeIdToCompletedTSN }.and {
                    hasSize(3)
                    getValue(store1Name).isEqualTo(777777)
                    getValue(store2Name).isEqualTo(1000)
                    getValue(store3Name).isEqualTo(777777)
                }

                get { this.storeIdToCommands }.and {
                    hasSize(3)
                    getValue(store1Name).containsExactly(
                        Command.put("foo", 1000, "bar"),
                        Command.put("hello", 1000, "world"),
                        Command.put("foo", 777777, "baz"),
                        Command.del("hello", 777777),
                    )
                    getValue(store2Name).containsExactly(
                        Command.put("e", 1000, "2.71828"),
                        Command.put("pi", 1000, "3.1415"),
                    )
                    getValue(store3Name).containsExactly(
                        Command.put("jane", 1000, "doe"),
                        Command.put("john", 1000, "doe"),
                        Command.put("sarah", 777777, "doe"),
                    )
                }
            }
        }
    }

    @ParameterizedTest
    @VirtualFileSystemTest
    fun canShortenWAL(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val walDirectory = vfs.directory(LSM4KStructure.WRITE_AHEAD_LOG_DIR_NAME)
            walDirectory.mkdirs()

            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0000${WRITE_AHEAD_LOG_FILE_SUFFIX}").create()
            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0001${WRITE_AHEAD_LOG_FILE_SUFFIX}").create()
            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0002${WRITE_AHEAD_LOG_FILE_SUFFIX}").create()
            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0003${WRITE_AHEAD_LOG_FILE_SUFFIX}").create()

            val wal = WriteAheadLog(walDirectory)
            wal.deleteWalFilesWithSequenceNumberLowerThan(2)

            // for a low watermark of 2000:
            // - The file 0000 can safely be deleted, because the next-higher file has timestamp 1999 (less than low watermark)
            // - The file 0001 can safely be deleted, because the next-higher file has timestamp 1999 (less than low watermark)
            // - The file 0002 can not be deleted, because the next-higher file has sequence number 4 (higher than low watermark) so our file might contain data after 0003.
            // - The file 0003 cannot be deleted because it's sequence number is higher than the low watermark.

            expectThat(walDirectory).get { this.list() }.containsExactlyInAnyOrder(
                "${WRITE_AHEAD_LOG_FILE_PREFIX}Base${WRITE_AHEAD_LOG_FILE_SUFFIX}",
                "${WRITE_AHEAD_LOG_FILE_PREFIX}0002${WRITE_AHEAD_LOG_FILE_SUFFIX}",
                "${WRITE_AHEAD_LOG_FILE_PREFIX}0003${WRITE_AHEAD_LOG_FILE_SUFFIX}",
            )
        }
    }

    @ParameterizedTest
    @VirtualFileSystemTest
    fun canShortenWALLargeTransaction(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val walDirectory = vfs.directory(LSM4KStructure.WRITE_AHEAD_LOG_DIR_NAME)
            walDirectory.mkdirs()

            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0000${WRITE_AHEAD_LOG_FILE_SUFFIX}").create()
            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0001${WRITE_AHEAD_LOG_FILE_SUFFIX}").overwrite(
                TransactionStartEntry(1875),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("x", 1875, "1")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("y", 1875, "2")),
                TransactionCommitEntry(1875),

                TransactionStartEntry(1899),
            )
            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0002${WRITE_AHEAD_LOG_FILE_SUFFIX}").overwrite(
                TransactionCommandEntry(StoreId.of("foo"), Command.put("x", 1899, "10")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("y", 1899, "20")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("a", 1899, "30")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("b", 1899, "40")),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("x", 1899, "100")),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("y", 1899, "200")),
            )
            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0003${WRITE_AHEAD_LOG_FILE_SUFFIX}").overwrite(
                TransactionCommandEntry(StoreId.of("foo"), Command.put("x", 1899, "777")),
                TransactionCommitEntry(1899),
                TransactionStartEntry(1875),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("x", 2001, "1000")),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("y", 2001, "2000")),
                TransactionCommitEntry(2001),
            )
            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0004${WRITE_AHEAD_LOG_FILE_SUFFIX}").create()


            val wal = WriteAheadLog(walDirectory)
            wal.deleteWalFilesWithSequenceNumberLowerThan(3)

            // for a low watermark of 0003:
            // - The file 0000 can safely be deleted
            // - The file 0001 cannot be deleted because it contains data for the opening partial transaction in 0004.
            // - The file 0002 cannot be deleted because it contains data for the opening partial transaction in 0004.
            // - The file 0003 cannot be deleted because it matches the low watermark sequence number exactly.
            // - The file 0004 cannot be deleted because it's sequence number is higher than the low watermark.

            expectThat(walDirectory).get { this.list() }.containsExactlyInAnyOrder(
                "${WRITE_AHEAD_LOG_FILE_PREFIX}Base${WRITE_AHEAD_LOG_FILE_SUFFIX}",
                "${WRITE_AHEAD_LOG_FILE_PREFIX}0001${WRITE_AHEAD_LOG_FILE_SUFFIX}",
                "${WRITE_AHEAD_LOG_FILE_PREFIX}0002${WRITE_AHEAD_LOG_FILE_SUFFIX}",
                "${WRITE_AHEAD_LOG_FILE_PREFIX}0003${WRITE_AHEAD_LOG_FILE_SUFFIX}",
                "${WRITE_AHEAD_LOG_FILE_PREFIX}0004${WRITE_AHEAD_LOG_FILE_SUFFIX}",
            )

            val walBaseContent = walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}Base${WRITE_AHEAD_LOG_FILE_SUFFIX}").withInputStream { it.readLittleEndianLongOrNull() }
            expectThat(walBaseContent).describedAs("WAL Base Content").isEqualTo(0L)
        }
    }

    @ParameterizedTest
    @VirtualFileSystemTest
    fun canCleanupTruncatedWALFile(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val walDirectory = vfs.directory(LSM4KStructure.WRITE_AHEAD_LOG_DIR_NAME)
            walDirectory.mkdirs()

            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0000${WRITE_AHEAD_LOG_FILE_SUFFIX}").create().overwrite(
                TransactionStartEntry(1875),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("x", 1875, "1")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("y", 1875, "2")),
                TransactionCommitEntry(1875),

                TransactionStartEntry(1899),
            )

            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0001${WRITE_AHEAD_LOG_FILE_SUFFIX}").overwrite(
                TransactionCommandEntry(StoreId.of("foo"), Command.put("x", 1899, "10")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("y", 1899, "20")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("a", 1899, "30")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("b", 1899, "40")),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("x", 1899, "100")),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("y", 1899, "200")),
            )
            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0002${WRITE_AHEAD_LOG_FILE_SUFFIX}").overwrite(
                TransactionCommandEntry(StoreId.of("foo"), Command.put("x", 1899, "777")),
                TransactionCommitEntry(1899),
                TransactionStartEntry(1875),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("x", 2001, "1000")),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("y", 2001, "2000")),
                TransactionCommitEntry(2001),
                TransactionStartEntry(2002),
            )
            val wal0004 = walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0003${WRITE_AHEAD_LOG_FILE_SUFFIX}").overwrite(
                TransactionCommandEntry(StoreId.of("baz"), Command.put("x", 2002, "111")),
                TransactionCommitEntry(2002),
                TransactionStartEntry(2003),
                TransactionCommandEntry(StoreId.of("baz"), Command.put("x", 2003, "1000")),
                TransactionCommandEntry(StoreId.of("baz"), Command.put("y", 2003, "2000")),
                TransactionCommitEntry(2003),
            )

            // cut off the last 10 bytes
            wal0004.truncateAfter(wal0004.length - 10)


            val wal = WriteAheadLog(walDirectory)
            wal.performStartupRecoveryCleanup(2001)

            // check the contents of wal2003
            val entries = wal0004.withInputStream { WALEntry.readStreaming(it).toList() }

            expectThat(entries).containsExactly(
                TransactionCommandEntry(StoreId.of("baz"), Command.put("x", 2002, "111")),
                TransactionCommitEntry(2002),
            )
        }
    }

    @ParameterizedTest
    @VirtualFileSystemTest
    fun canCleanupMissingWALFile(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val walDirectory = vfs.directory(LSM4KStructure.WRITE_AHEAD_LOG_DIR_NAME)
            walDirectory.mkdirs()

            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0000${WRITE_AHEAD_LOG_FILE_SUFFIX}").create().overwrite(
                TransactionStartEntry(1875),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("x", 1875, "1")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("y", 1875, "2")),
                TransactionCommitEntry(1875),

                TransactionStartEntry(1899),
            )

            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0001${WRITE_AHEAD_LOG_FILE_SUFFIX}").overwrite(
                TransactionCommandEntry(StoreId.of("foo"), Command.put("x", 1899, "10")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("y", 1899, "20")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("a", 1899, "30")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("b", 1899, "40")),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("x", 1899, "100")),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("y", 1899, "200")),
            )
            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0002${WRITE_AHEAD_LOG_FILE_SUFFIX}").overwrite(
                TransactionCommandEntry(StoreId.of("foo"), Command.put("x", 1899, "777")),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("x", 1899, "1000")),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("y", 1899, "2000")),
            )


            val wal = WriteAheadLog(walDirectory)
            wal.performStartupRecoveryCleanup(1875)

            expectThat(walDirectory).get { this.list() }.containsExactlyInAnyOrder(
                "${WRITE_AHEAD_LOG_FILE_PREFIX}0000${WRITE_AHEAD_LOG_FILE_SUFFIX}",
            )
        }
    }


    @ParameterizedTest
    @VirtualFileSystemTest
    fun canDetectMissingWALFileAtTheEnd(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val walDirectory = vfs.directory(LSM4KStructure.WRITE_AHEAD_LOG_DIR_NAME)
            walDirectory.mkdirs()

            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0000${WRITE_AHEAD_LOG_FILE_SUFFIX}").create().overwrite(
                TransactionStartEntry(1875),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("x", 1875, "1")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("y", 1875, "2")),
                TransactionCommitEntry(1875),

                TransactionStartEntry(1899),
            )

            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0001${WRITE_AHEAD_LOG_FILE_SUFFIX}").overwrite(
                TransactionCommandEntry(StoreId.of("foo"), Command.put("x", 1899, "10")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("y", 1899, "20")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("a", 1899, "30")),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("b", 1899, "40")),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("x", 1899, "100")),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("y", 1899, "200")),
                TransactionCommitEntry(1899),
            )
            val wal0003 = walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}0002${WRITE_AHEAD_LOG_FILE_SUFFIX}").overwrite(
                TransactionStartEntry(2001),
                TransactionCommandEntry(StoreId.of("foo"), Command.put("x", 2001, "777")),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("x", 2001, "1000")),
                TransactionCommandEntry(StoreId.of("bar"), Command.put("y", 2001, "2000")),
                TransactionCommitEntry(2001),
            )

            // simulate the loss of WAL file 0003
            wal0003.delete()

            expectThrows<WriteAheadLogCorruptedException> {
                val wal = WriteAheadLog(walDirectory)
                wal.performStartupRecoveryCleanup(2001)
            }.get { this.message }.isEqualTo("There is no WAL file which reflects persisted transaction with Serial Number 2001!")
        }
    }

    @Test
    fun canSerializeAndDeserializeWALEntries() {
        val entries = listOf(
            TransactionStartEntry(1),
            TransactionCommandEntry(StoreId.of("test/mystore"), Command.put("foo", 1, "bar")),
            TransactionCommandEntry(StoreId.of("test/mystore"), Command.put("lorem", 1, "ipsum")),
            TransactionCommandEntry(StoreId.of("test/mystore"), Command.del("xyz", 1)),
            TransactionCommitEntry(1),

            TransactionStartEntry(1234),
            TransactionCommandEntry(StoreId.of("foobar"), Command.put("foo", 1234, "baz")),
            TransactionCommitEntry(1234),
        )

        val serializedForm = ByteArrayOutputStream().use { baos ->
            WALEntry.writeStreaming(entries.asSequence(), baos)
            baos.toByteArray()
        }

        val deserializedEntries = ByteArrayInputStream(serializedForm).use { bais ->
            WALEntry.readStreaming(bais).toList()
        }

        expectThat(deserializedEntries).containsExactly(entries)
    }

    private class FakeTransaction(
        val commitTSN: TSN,
        val storeIdToCommands: Map<StoreId, List<Command>>,
    ) {

        fun getChangesAsSequence(): Sequence<Pair<StoreId, Command>> {
            return storeIdToCommands.entries.asSequence()
                .flatMap { (storeId, commands) ->
                    commands.asSequence()
                        .map { Pair(storeId, it) }
                }
        }

    }

    private class WALContent(
        val storeIdToCompletedTSN: Map<StoreId, TSN>,
        val storeIdToCommands: Map<StoreId, List<Command>>,
    )

    private fun WriteAheadLog.addCommittedTransaction(transaction: FakeTransaction) {
        this.addCommittedTransaction(commitTSN = transaction.commitTSN, transactionChanges = transaction.getChangesAsSequence())
    }

    private fun WriteAheadLog.readContent(): WALContent {
        // we use an unlimited buffer here because the tests
        // should only contain small datasets, and we want to
        // read the entire (logical) content of the WAL for
        // easy analysis. The flushBuffer lambda will be
        // called exactly once.
        val buffer = WALReadBuffer(
            maxSizeInBytes = Long.MAX_VALUE,
            storeIdToLowWatermark = emptyMap(),
        )
        var walContent: WALContent? = null
        this.readWalStreaming(buffer) {
            // flush buffer
            walContent = WALContent(
                storeIdToCompletedTSN = buffer.getModifiedStoreIds()
                    .associateWith { buffer.getCompletedTSNForStore(it)!! },
                storeIdToCommands = buffer.getModifiedStoreIds()
                    .associateWith { buffer.getCommandsForStore(it) }
                    .mapValues { (_, commands) ->
                        // we sort the commands here for testing purposes to make the
                        // assertions more precise / easier to manage. For production
                        // use, this is not necessary, because the memtables will sort
                        // the incoming data automatically.
                        commands.sortedWith(compareBy({ it.tsn }, { it.key }))
                    }
            )
        }
        return walContent!!
    }

    private fun VirtualReadWriteFile.overwrite(vararg entries: WALEntry): VirtualReadWriteFile {
        this.withOverWriter { overWriter ->
            val out = overWriter.outputStream
            WALEntry.writeStreaming(entries.asSequence(), out)
            overWriter.commit()
        }
        return this
    }

}


