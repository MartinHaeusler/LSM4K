package org.chronos.chronostore.test.cases.wal

import com.google.common.io.CountingOutputStream
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.io.structure.ChronoStoreStructure.WRITE_AHEAD_LOG_FILE_PREFIX
import org.chronos.chronostore.io.structure.ChronoStoreStructure.WRITE_AHEAD_LOG_FILE_SUFFIX
import org.chronos.chronostore.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.test.util.VFSMode
import org.chronos.chronostore.test.util.VirtualFileSystemTest
import org.chronos.chronostore.util.IOExtensions.withInputStream
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.bytes.BasicBytes
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.unit.MiB
import org.chronos.chronostore.wal.WriteAheadLog
import org.chronos.chronostore.wal.WriteAheadLogEntry
import org.chronos.chronostore.wal.WriteAheadLogTransaction
import org.chronos.chronostore.wal2.WriteAheadLog2
import org.chronos.chronostore.wal2.WriteAheadLogFormat
import strikt.api.expectThat
import strikt.assertions.*
import java.util.*

class WriteAheadLogTest {

    @VirtualFileSystemTest
    fun canWriteAndReadTransactions(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val wal = WriteAheadLog2(vfs.directory("wal"), CompressionAlgorithm.SNAPPY, 128.MiB.bytes)

            val tx1Id = UUID.randomUUID()
            val tx2Id = UUID.randomUUID()
            val tx3Id = UUID.randomUUID()
            val store1Name = StoreId.of("d272b6e7-e1bc-4d6f-bf04-93e36445b279")
            val store2Name = StoreId.of("508e1b3c-3383-47c4-bdfe-13175c88a146")
            val store3Name = StoreId.of("c4071866-48fe-48df-af6b-5e29f1e833cb")

            val tx1 = WriteAheadLogTransaction(
                transactionId = tx1Id,
                commitTimestamp = 1000,
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
                commitMetadata = BasicBytes("John was here")
            )

            val tx2 = WriteAheadLogTransaction(
                transactionId = tx2Id,
                commitTimestamp = 1234,
                storeIdToCommands = emptyMap(),
                commitMetadata = BasicBytes("Empty Commit")
            )


            val tx3 = WriteAheadLogTransaction(
                transactionId = tx3Id,
                commitTimestamp = 777777,
                storeIdToCommands = mapOf(
                    store1Name to listOf(
                        Command.del(BasicBytes("hello"), 777777),
                        Command.put(BasicBytes("foo"), 777777, BasicBytes("baz")),
                    ),
                    store3Name to listOf(
                        Command.put(BasicBytes("sarah"), 777777, BasicBytes("doe")),
                    )
                ),
                commitMetadata = BasicBytes("Jane did this")
            )

            wal.addCommittedTransaction(tx1)
            wal.addCommittedTransaction(tx2)
            wal.addCommittedTransaction(tx3)

            val readTx = mutableListOf<WriteAheadLogTransaction>()
            wal.readWalStreaming { readTx.add(it) }

            expectThat(readTx).hasSize(3).and {
                get(0).and {
                    get { this.transactionId }.isEqualTo(tx1Id)
                    get { this.commitTimestamp }.isEqualTo(1000)
                    get { this.commitMetadata }.isEqualTo(BasicBytes("John was here"))
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
                    get { this.commitTimestamp }.isEqualTo(1234)
                    get { this.commitMetadata }.isEqualTo(BasicBytes("Empty Commit"))
                    get { this.storeIdToCommands }.isEmpty()
                }
                get(2).and {
                    get { this.transactionId }.isEqualTo(tx3Id)
                    get { this.commitTimestamp }.isEqualTo(777777)
                    get { this.commitMetadata }.isEqualTo(BasicBytes("Jane did this"))
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
                            commitTimestamp = commitTimestamp1,
                            storeIdToCommands = mapOf(
                                storeId to listOf(
                                    Command.put("foo", commitTimestamp1, "bar"),
                                    Command.put("hello", commitTimestamp1, "world"),
                                )
                            ),
                            commitMetadata = Bytes.EMPTY
                        )
                    )

                    bytesCommit1 = cOut.count

                    WriteAheadLogFormat.writeTransaction(
                        out = cOut,
                        compressionAlgorithm = CompressionAlgorithm.SNAPPY,
                        tx = WriteAheadLogTransaction(
                            transactionId = tx2Id,
                            commitTimestamp = commitTimestamp2,
                            storeIdToCommands = mapOf(
                                storeId to listOf(
                                    Command.put("foo", commitTimestamp2, "baz"),
                                    Command.put("pi", commitTimestamp2, "3.1415")
                                )
                            ),
                            commitMetadata = Bytes.EMPTY
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
                println("Truncating WAL file to ${bytesToKeep} bytes...")

                // reset the file
                walFile.withOverWriter { overwriter ->
                    overwriter.outputStream.write(originalWalFileBytes)
                    overwriter.commit()
                }

                // truncate the file to the new length to "simulate" power outage / process kill during write.
                walFile.truncateAfter(bytesToKeep)

                expectThat(walFile).get { this.length }.isEqualTo(bytesToKeep)

                val wal = WriteAheadLog2(walDirectory, CompressionAlgorithm.SNAPPY, 128.MiB.bytes)
                // this should truncate our WAL file and get rid of the second commit (which is incomplete)
                wal.performStartupRecoveryCleanup { commitTimestamp1 }

                expectThat(walFile).get { this.length }.isEqualTo(bytesCommit1)

                val transactions = wal.allTransactions

                expectThat(transactions).single().and {
                    get { this.transactionId }.isEqualTo(tx1Id)
                    get { this.commitMetadata }.isEqualTo(Bytes.EMPTY)
                    get { this.storeIdToCommands.entries }.single().and {
                        get { this.key }.isEqualTo(storeId)
                        get { this.value }.hasSize(2).and {
                            one {
                                get { this.key }.isEqualTo(BasicBytes("foo"))
                                get { this.value }.isEqualTo(BasicBytes("bar"))
                                get { this.opCode }.isEqualTo(Command.OpCode.PUT)
                                get { this.timestamp }.isEqualTo(123456)
                            }
                            one {
                                get { this.key }.isEqualTo(BasicBytes("hello"))
                                get { this.value }.isEqualTo(BasicBytes("world"))
                                get { this.opCode }.isEqualTo(Command.OpCode.PUT)
                                get { this.timestamp }.isEqualTo(123456)
                            }
                        }
                    }
                }
            }
        }
    }

    @VirtualFileSystemTest
    fun canCompactWAL(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val walFile = vfs.file("test.wal")

            // TODO: Rewrite to WalV2 and get rid of WALV1!


            val tx1Id = UUID.fromString("d5215e86-d787-4c50-8dea-a8da76771a03")
            val tx2Id = UUID.fromString("60fe702f-c0f2-44b6-8216-d67a6256d4ed")
            val tx3Id = UUID.fromString("996c8fb9-b659-4307-b574-36e7a3c21bd0")

            val storeId = StoreId.of("6d529fdd-1553-4cf4-9f97-e3d1a52cef3e")

            walFile.withOverWriter { overWriter ->
                val outputStream = overWriter.outputStream
                WriteAheadLogEntry.beginTransaction(tx1Id).writeTo(outputStream)
                WriteAheadLogEntry.put(tx1Id, storeId, BasicBytes("foo"), BasicBytes("bar")).writeTo(outputStream)
                WriteAheadLogEntry.put(tx1Id, storeId, BasicBytes("hello"), BasicBytes("world")).writeTo(outputStream)
                WriteAheadLogEntry.commit(tx1Id, 123456, Bytes.EMPTY).writeTo(outputStream)

                WriteAheadLogEntry.beginTransaction(tx2Id).writeTo(outputStream)
                WriteAheadLogEntry.put(tx2Id, storeId, BasicBytes("foo"), BasicBytes("baz")).writeTo(outputStream)
                WriteAheadLogEntry.commit(tx2Id, 555555, Bytes.EMPTY).writeTo(outputStream)

                WriteAheadLogEntry.beginTransaction(tx3Id).writeTo(outputStream)
                WriteAheadLogEntry.put(tx3Id, storeId, BasicBytes("lorem"), BasicBytes("ipsum")).writeTo(outputStream)
                WriteAheadLogEntry.commit(tx3Id, 777777, Bytes.EMPTY).writeTo(outputStream)
                overWriter.commit()
            }

            val wal = WriteAheadLog(walFile)

            wal.compactWal { tx ->
                tx.transactionId != tx2Id
            }

            val transactions = wal.allTransactions

            expectThat(transactions).single().and {
                get { this.transactionId }.isEqualTo(tx2Id)
                get { this.commitMetadata }.isEqualTo(Bytes.EMPTY)
                get { this.storeIdToCommands.entries }.single().and {
                    get { this.key }.isEqualTo(storeId)
                    get { this.value }.single().and {
                        get { this.key }.isEqualTo(BasicBytes("foo"))
                        get { this.value }.isEqualTo(BasicBytes("baz"))
                        get { this.opCode }.isEqualTo(Command.OpCode.PUT)
                        get { this.timestamp }.isEqualTo(555555)
                    }
                }
            }
        }
    }


    private val WriteAheadLog.allTransactions: List<WriteAheadLogTransaction>
        get() {
            val transactions = mutableListOf<WriteAheadLogTransaction>()
            readWal { transactions += it }
            return transactions
        }

    private val WriteAheadLog2.allTransactions: List<WriteAheadLogTransaction>
        get() {
            val transactions = mutableListOf<WriteAheadLogTransaction>()
            readWalStreaming { transactions += it }
            return transactions
        }


}