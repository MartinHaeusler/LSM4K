package org.chronos.chronostore.test.cases.wal

import com.google.common.io.CountingOutputStream
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.io.structure.ChronoStoreStructure
import org.chronos.chronostore.io.structure.ChronoStoreStructure.WRITE_AHEAD_LOG_FILE_PREFIX
import org.chronos.chronostore.io.structure.ChronoStoreStructure.WRITE_AHEAD_LOG_FILE_SUFFIX
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.test.util.VFSMode
import org.chronos.chronostore.test.util.VirtualFileSystemTest
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.bytes.BasicBytes
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.wal.WALFile
import org.chronos.chronostore.wal.WriteAheadLogTransaction
import org.chronos.chronostore.wal.WriteAheadLog
import org.chronos.chronostore.wal.WriteAheadLogFormat
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.*
import java.util.*

class WriteAheadLogTest {

    @VirtualFileSystemTest
    fun canWriteAndReadTransactions(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val wal = WriteAheadLog(vfs.directory("wal"))

            val tx1Id = UUID.randomUUID()
            val tx2Id = UUID.randomUUID()
            val tx3Id = UUID.randomUUID()
            val store1Name = StoreId.of("d272b6e7-e1bc-4d6f-bf04-93e36445b279")
            val store2Name = StoreId.of("508e1b3c-3383-47c4-bdfe-13175c88a146")
            val store3Name = StoreId.of("c4071866-48fe-48df-af6b-5e29f1e833cb")

            val tx1 = WriteAheadLogTransaction(
                transactionId = tx1Id,
                commitTSN = 1000,
                storeIdToCommands = mapOf(
                    store1Name to listOf(
                        Command.put(BasicBytes("hello"), 1000, BasicBytes("world")),
                        Command.put(BasicBytes("foo"), 1000, BasicBytes("bar")),
                    ),
                    store2Name to listOf(
                        Command.put(BasicBytes("pi"), 1000, BasicBytes("3.1415")),
                        Command.put(BasicBytes("e"), 1000, BasicBytes("2.71828"))
                    ),
                    store3Name to listOf(
                        Command.put(BasicBytes("john"), 1000, BasicBytes("doe")),
                        Command.put(BasicBytes("jane"), 1000, BasicBytes("doe")),
                    )
                ),
            )

            val tx2 = WriteAheadLogTransaction(
                transactionId = tx2Id,
                commitTSN = 1234,
                storeIdToCommands = emptyMap(),
            )


            val tx3 = WriteAheadLogTransaction(
                transactionId = tx3Id,
                commitTSN = 777777,
                storeIdToCommands = mapOf(
                    store1Name to listOf(
                        Command.del(BasicBytes("hello"), 777777),
                        Command.put(BasicBytes("foo"), 777777, BasicBytes("baz")),
                    ),
                    store3Name to listOf(
                        Command.put(BasicBytes("sarah"), 777777, BasicBytes("doe")),
                    )
                ),
            )

            wal.addCommittedTransaction(tx1)
            wal.addCommittedTransaction(tx2)
            wal.addCommittedTransaction(tx3)

            val readTx = mutableListOf<WriteAheadLogTransaction>()
            wal.readWalStreaming { readTx.add(it) }

            expectThat(readTx).hasSize(3).and {
                get(0).and {
                    get { this.transactionId }.isEqualTo(tx1Id)
                    get { this.commitTSN }.isEqualTo(1000)
                    get { this.storeIdToCommands }.hasSize(3).and {
                        get(store1Name).isNotNull().hasSize(2).and {
                            get(0).isEqualTo(Command.put(BasicBytes("hello"), 1000, BasicBytes("world")))
                            get(1).isEqualTo(Command.put(BasicBytes("foo"), 1000, BasicBytes("bar")))
                        }
                        get(store2Name).isNotNull().hasSize(2).and {
                            get(0).isEqualTo(Command.put(BasicBytes("pi"), 1000, BasicBytes("3.1415")))
                            get(1).isEqualTo(Command.put(BasicBytes("e"), 1000, BasicBytes("2.71828")))
                        }
                        get(store3Name).isNotNull().hasSize(2).and {
                            get(0).isEqualTo(Command.put(BasicBytes("john"), 1000, BasicBytes("doe")))
                            get(1).isEqualTo(Command.put(BasicBytes("jane"), 1000, BasicBytes("doe")))
                        }
                    }
                }
                get(1).and {
                    get { this.transactionId }.isEqualTo(tx2Id)
                    get { this.commitTSN }.isEqualTo(1234)
                    get { this.storeIdToCommands }.isEmpty()
                }
                get(2).and {
                    get { this.transactionId }.isEqualTo(tx3Id)
                    get { this.commitTSN }.isEqualTo(777777)
                    get { this.storeIdToCommands }.hasSize(2).and {
                        get(store1Name).isNotNull().hasSize(2).and {
                            get(0).isEqualTo(Command.del(BasicBytes("hello"), 777777))
                            get(1).isEqualTo(Command.put(BasicBytes("foo"), 777777, BasicBytes("baz")))
                        }
                        get(store3Name).isNotNull().hasSize(1).and {
                            get(0).isEqualTo(Command.put(BasicBytes("sarah"), 777777, BasicBytes("doe")))
                        }
                    }
                }
            }
        }
    }

    @VirtualFileSystemTest
    fun canCleanUpIncompleteTransactionsAtTheEnd(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val commitTimestamp1 = 123456L
            val commitTimestamp2 = 123777L

            val walDirectory = vfs.directory("wal")
            walDirectory.mkdirs()
            val walFile = walDirectory.file("$WRITE_AHEAD_LOG_FILE_PREFIX${commitTimestamp1}$WRITE_AHEAD_LOG_FILE_SUFFIX")
            walFile.create()

            val tx1Id = UUID.randomUUID()
            val tx2Id = UUID.randomUUID()

            val storeId = StoreId.of("b6b8388f-0e1d-444a-9254-86fe3d9e6b02")

            var bytesCommit1: Long = -1L
            var bytesCommit2: Long = -1L

            walFile.append { out ->
                CountingOutputStream(out).use { cOut ->
                    WriteAheadLogFormat.writeTransaction(
                        out = cOut,
                        compressionAlgorithm = CompressionAlgorithm.SNAPPY,
                        tx = WriteAheadLogTransaction(
                            transactionId = tx1Id,
                            commitTSN = commitTimestamp1,
                            storeIdToCommands = mapOf(
                                storeId to listOf(
                                    Command.put("foo", commitTimestamp1, "bar"),
                                    Command.put("hello", commitTimestamp1, "world"),
                                )
                            ),
                        )
                    )

                    bytesCommit1 = cOut.count

                    WriteAheadLogFormat.writeTransaction(
                        out = cOut,
                        compressionAlgorithm = CompressionAlgorithm.SNAPPY,
                        tx = WriteAheadLogTransaction(
                            transactionId = tx2Id,
                            commitTSN = commitTimestamp2,
                            storeIdToCommands = mapOf(
                                storeId to listOf(
                                    Command.put("foo", commitTimestamp2, "baz"),
                                    Command.put("pi", commitTimestamp2, "3.1415")
                                )
                            ),
                        )
                    )

                    bytesCommit2 = cOut.count
                }
            }

            expectThat(bytesCommit1).isGreaterThan(0L)
            expectThat(bytesCommit2).isGreaterThan(bytesCommit1)

            val originalWalFileBytes = walFile.withInputStream { it.readAllBytes() }

            // here we simulate truncation of the file at EVERY byte of the transaction. We must be able
            // to detect cut-offs at *any* point of the file content and act accordingly.
            for (bytesToKeep in ((bytesCommit2 - 1).downTo(bytesCommit1 + 1))) {
                // reset the file
                walFile.withOverWriter { overwriter ->
                    overwriter.outputStream.write(originalWalFileBytes)
                    overwriter.commit()
                }

                // truncate the file to the new length to "simulate" power outage / process kill during write.
                walFile.truncateAfter(bytesToKeep)

                expectThat(walFile).get { this.length }.isEqualTo(bytesToKeep)

                val wal = WriteAheadLog(walDirectory)
                // this should truncate our WAL file and get rid of the second commit (which is incomplete)
                wal.performStartupRecoveryCleanup { commitTimestamp1 }

                expectThat(walFile).get { this.length }.isEqualTo(bytesCommit1)

                val transactions = wal.allTransactions

                expectThat(transactions).single().and {
                    get { this.transactionId }.isEqualTo(tx1Id)
                    get { this.storeIdToCommands.entries }.single().and {
                        get { this.key }.isEqualTo(storeId)
                        get { this.value }.hasSize(2).and {
                            one {
                                get { this.key }.isEqualTo(BasicBytes("foo"))
                                get { this.value }.isEqualTo(BasicBytes("bar"))
                                get { this.opCode }.isEqualTo(Command.OpCode.PUT)
                                get { this.tsn }.isEqualTo(123456)
                            }
                            one {
                                get { this.key }.isEqualTo(BasicBytes("hello"))
                                get { this.value }.isEqualTo(BasicBytes("world"))
                                get { this.opCode }.isEqualTo(Command.OpCode.PUT)
                                get { this.tsn }.isEqualTo(123456)
                            }
                        }
                    }
                }
            }
        }
    }

    @VirtualFileSystemTest
    fun canShortenWAL(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val walDirectory = vfs.directory(ChronoStoreStructure.WRITE_AHEAD_LOG_DIR_NAME)
            walDirectory.mkdirs()

            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}1234${WRITE_AHEAD_LOG_FILE_SUFFIX}").create()
            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}1875${WRITE_AHEAD_LOG_FILE_SUFFIX}").create()
            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}1999${WRITE_AHEAD_LOG_FILE_SUFFIX}").create()
            walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}2001${WRITE_AHEAD_LOG_FILE_SUFFIX}").create()

            val wal = WriteAheadLog(walDirectory)

            wal.shorten(lowWatermarkTSN = 2000)

            // for a low watermark of 2000:
            // - The file 1234 can safely be deleted, because the next-higher file has timestamp 1999 (less than low watermark)
            // - The file 1875 can safely be deleted, because the next-higher file has timestamp 1999 (less than low watermark)
            // - The file 1999 can not be deleted, because the next-higher file has timestamp 2001 (higher than low watermark) so our file might contain data after 2000.
            // - The file 2001 cannot be deleted because it's timestamp is higher than the low watermark.

            expectThat(walDirectory).get { this.list() }.containsExactlyInAnyOrder(
                "${WRITE_AHEAD_LOG_FILE_PREFIX}1999${WRITE_AHEAD_LOG_FILE_SUFFIX}",
                "${WRITE_AHEAD_LOG_FILE_PREFIX}2001${WRITE_AHEAD_LOG_FILE_SUFFIX}"
            )
        }
    }

    @VirtualFileSystemTest
    fun canGenerateAndValidateChecksumForWALFile(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val walDirectory = vfs.directory(ChronoStoreStructure.WRITE_AHEAD_LOG_DIR_NAME)
            walDirectory.mkdirs()

            val walFile1 = WALFile(walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}1234${WRITE_AHEAD_LOG_FILE_SUFFIX}").create(), 1234)
            val walFile2 = WALFile(walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}5678${WRITE_AHEAD_LOG_FILE_SUFFIX}").create(), 5678)

            walFile1.file.withOverWriter { overWriter ->
                WriteAheadLogFormat.writeTransaction(
                    out = overWriter.outputStream,
                    compressionAlgorithm = CompressionAlgorithm.SNAPPY,
                    tx = WriteAheadLogTransaction(
                        transactionId = TransactionId.randomUUID(),
                        commitTSN = 1300,
                        storeIdToCommands = mapOf(
                            StoreId.of("hello") to listOf(
                                Command.put("foo", 1300, "baz"),
                                Command.put("pi", 1300, "3.1415")
                            )
                        ),
                    )
                )
                overWriter.commit()
            }

            val wal = WriteAheadLog(walDirectory)

            // we do the following twice because "generateChecksumsForCompletedFiles()"
            // should not perform any changes if every eligible WAL file has a checksum
            // associated with it.
            repeat(2) {
                wal.generateChecksumsForCompletedFiles()

                expect {
                    that(walFile1) {
                        get { this.checksumFile }.and {
                            get { this.name.endsWith(".md5") }
                            get { this.exists() }.isTrue()
                            get { this.withInputStream { it.bufferedReader().use { r -> r.readText() } } }.isNotBlank()
                        }
                        get { this.validateChecksum() }.isTrue()
                    }
                    that(walFile2) {
                        get { this.checksumFile }.and {
                            get { this.name.endsWith(".md5") }
                            get { this.exists() }.isFalse()
                        }
                        get { this.validateChecksum() }.isNull()
                    }
                }
            }
        }
    }

    @VirtualFileSystemTest
    fun deletingWALFileAlsoDeletesChecksumFile(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val walDirectory = vfs.directory(ChronoStoreStructure.WRITE_AHEAD_LOG_DIR_NAME)
            walDirectory.mkdirs()

            val walFile1 = WALFile(walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}1234${WRITE_AHEAD_LOG_FILE_SUFFIX}").create(), 1234)
            val walFile2 = WALFile(walDirectory.file("${WRITE_AHEAD_LOG_FILE_PREFIX}5678${WRITE_AHEAD_LOG_FILE_SUFFIX}").create(), 5678)

            walFile1.file.withOverWriter { overWriter ->
                WriteAheadLogFormat.writeTransaction(
                    out = overWriter.outputStream,
                    compressionAlgorithm = CompressionAlgorithm.SNAPPY,
                    tx = WriteAheadLogTransaction(
                        transactionId = TransactionId.randomUUID(),
                        commitTSN = 1300,
                        storeIdToCommands = mapOf(
                            StoreId.of("hello") to listOf(
                                Command.put("foo", 1300, "baz"),
                                Command.put("pi", 1300, "3.1415")
                            )
                        ),
                    )
                )
                overWriter.commit()
            }

            val wal = WriteAheadLog(walDirectory)
            wal.generateChecksumsForCompletedFiles()

            expect {
                that(walFile1) {
                    get { this.checksumFile }.and {
                        get { this.name.endsWith(".md5") }
                        get { this.exists() }.isTrue()
                        get { this.withInputStream { it.bufferedReader().use { r -> r.readText() } } }.isNotBlank()
                    }
                    get { this.validateChecksum() }.isTrue()
                }
                that(walFile2) {
                    get { this.checksumFile }.and {
                        get { this.name.endsWith(".md5") }
                        get { this.exists() }.isFalse()
                    }
                    get { this.validateChecksum() }.isNull()
                }
            }

            walFile1.delete()
            expectThat(walFile1).get { this.checksumFile }.get { this.exists() }.isFalse()
        }
    }

    private val WriteAheadLog.allTransactions: List<WriteAheadLogTransaction>
        get() {
            val transactions = mutableListOf<WriteAheadLogTransaction>()
            readWalStreaming { transactions += it }
            return transactions
        }


}