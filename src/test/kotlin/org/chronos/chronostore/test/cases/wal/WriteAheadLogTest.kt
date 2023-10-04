package org.chronos.chronostore.test.cases.wal

import org.chronos.chronostore.io.vfs.VirtualReadWriteFile.Companion.withOverWriter
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.test.util.VFSMode
import org.chronos.chronostore.test.util.VirtualFileSystemTest
import org.chronos.chronostore.util.bytes.BasicBytes
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.wal.WriteAheadLog
import org.chronos.chronostore.wal.WriteAheadLogEntry
import org.chronos.chronostore.wal.WriteAheadLogTransaction
import strikt.api.expectThat
import strikt.assertions.*
import java.util.*

class WriteAheadLogTest {

    @VirtualFileSystemTest
    fun canWriteAndReadTransactions(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val wal = WriteAheadLog(vfs.file("test.wal"))

            val tx1Id = UUID.randomUUID()
            val tx2Id = UUID.randomUUID()
            val tx3Id = UUID.randomUUID()
            val store1Id = UUID.randomUUID()
            val store2Id = UUID.randomUUID()
            val store3Id = UUID.randomUUID()

            val tx1 = WriteAheadLogTransaction(
                transactionId = tx1Id,
                commitTimestamp = 1000,
                storeIdToCommands = mapOf(
                    store1Id to listOf(
                        Command.put(BasicBytes("hello"), 1000, BasicBytes("world")),
                        Command.put(BasicBytes("foo"), 1000, BasicBytes("bar")),
                    ),
                    store2Id to listOf(
                        Command.put(BasicBytes("pi"), 1000, BasicBytes("3.1415")),
                        Command.put(BasicBytes("e"), 1000, BasicBytes("2.71828"))
                    ),
                    store3Id to listOf(
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
                    store1Id to listOf(
                        Command.del(BasicBytes("hello"), 777777),
                        Command.put(BasicBytes("foo"), 777777, BasicBytes("baz")),
                    ),
                    store3Id to listOf(
                        Command.put(BasicBytes("sarah"), 777777, BasicBytes("doe")),
                    )
                ),
                commitMetadata = BasicBytes("Jane did this")
            )

            wal.addCommittedTransaction(tx1)
            wal.addCommittedTransaction(tx2)
            wal.addCommittedTransaction(tx3)

            val readTx = mutableListOf<WriteAheadLogTransaction>()
            wal.readWal { readTx.add(it) }

            expectThat(readTx).hasSize(3).and {
                get(0).and {
                    get { this.transactionId }.isEqualTo(tx1Id)
                    get { this.commitTimestamp }.isEqualTo(1000)
                    get { this.commitMetadata }.isEqualTo(BasicBytes("John was here"))
                    get { this.storeIdToCommands }.hasSize(3).and {
                        get(store1Id).isNotNull().hasSize(2).and {
                            get(0).isEqualTo(Command.put(BasicBytes("hello"), 1000, BasicBytes("world")))
                            get(1).isEqualTo(Command.put(BasicBytes("foo"), 1000, BasicBytes("bar")))
                        }
                        get(store2Id).isNotNull().hasSize(2).and {
                            get(0).isEqualTo(Command.put(BasicBytes("pi"), 1000, BasicBytes("3.1415")))
                            get(1).isEqualTo(Command.put(BasicBytes("e"), 1000, BasicBytes("2.71828")))
                        }
                        get(store3Id).isNotNull().hasSize(2).and {
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
                        get(store1Id).isNotNull().hasSize(2).and {
                            get(0).isEqualTo(Command.del(BasicBytes("hello"), 777777))
                            get(1).isEqualTo(Command.put(BasicBytes("foo"), 777777, BasicBytes("baz")))
                        }
                        get(store3Id).isNotNull().hasSize(1).and {
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
            val walFile = vfs.file("test.wal")

            val tx1Id = UUID.randomUUID()
            val tx2Id = UUID.randomUUID()

            val storeId = UUID.randomUUID()

            walFile.withOverWriter { overWriter ->
                val outputStream = overWriter.outputStream
                WriteAheadLogEntry.beginTransaction(tx1Id).writeTo(outputStream)
                WriteAheadLogEntry.put(tx1Id, storeId, BasicBytes("foo"), BasicBytes("bar")).writeTo(outputStream)
                WriteAheadLogEntry.put(tx1Id, storeId, BasicBytes("hello"), BasicBytes("world")).writeTo(outputStream)
                WriteAheadLogEntry.commit(tx1Id, 123456, Bytes.EMPTY).writeTo(outputStream)

                WriteAheadLogEntry.beginTransaction(tx2Id).writeTo(outputStream)
                WriteAheadLogEntry.put(tx2Id, storeId, BasicBytes("foo"), BasicBytes("baz")).writeTo(outputStream)
                overWriter.commit()
            }

            val wal = WriteAheadLog(walFile)
            wal.performStartupRecoveryCleanup()

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

    @VirtualFileSystemTest
    fun canCleanUpIncompleteTransactionsInTheMiddle(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val walFile = vfs.file("test.wal")

            val tx1Id = UUID.randomUUID()
            val tx2Id = UUID.randomUUID()
            val tx3Id = UUID.randomUUID()

            val storeId = UUID.randomUUID()

            walFile.withOverWriter { overWriter ->
                val outputStream = overWriter.outputStream
                WriteAheadLogEntry.beginTransaction(tx1Id).writeTo(outputStream)
                WriteAheadLogEntry.put(tx1Id, storeId, BasicBytes("foo"), BasicBytes("bar")).writeTo(outputStream)
                WriteAheadLogEntry.put(tx1Id, storeId, BasicBytes("hello"), BasicBytes("world")).writeTo(outputStream)
                WriteAheadLogEntry.commit(tx1Id, 123456, Bytes.EMPTY).writeTo(outputStream)

                WriteAheadLogEntry.beginTransaction(tx2Id).writeTo(outputStream)
                WriteAheadLogEntry.put(tx2Id, storeId, BasicBytes("foo"), BasicBytes("baz")).writeTo(outputStream)

                WriteAheadLogEntry.beginTransaction(tx3Id).writeTo(outputStream)
                WriteAheadLogEntry.put(tx3Id, storeId, BasicBytes("lorem"), BasicBytes("ipsum")).writeTo(outputStream)
                WriteAheadLogEntry.commit(tx3Id, 777777, Bytes.EMPTY).writeTo(outputStream)

                overWriter.commit()
            }

            val wal = WriteAheadLog(walFile)
            wal.performStartupRecoveryCleanup()

            val transactions = wal.allTransactions

            expectThat(transactions).hasSize(2).and {
                get(0).and {
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
                get(1).and {
                    get { this.transactionId }.isEqualTo(tx3Id)
                    get { this.commitMetadata }.isEqualTo(Bytes.EMPTY)
                    get { this.storeIdToCommands.entries }.single().and {
                        get { this.key }.isEqualTo(storeId)
                        get { this.value }.single().and {
                            get { this.key }.isEqualTo(BasicBytes("lorem"))
                            get { this.value }.isEqualTo(BasicBytes("ipsum"))
                            get { this.opCode }.isEqualTo(Command.OpCode.PUT)
                            get { this.timestamp }.isEqualTo(777777)
                        }
                    }
                }
            }
        }
    }

    @VirtualFileSystemTest
    fun canCleanUpInvalidCommitEntry(vfsMode: VFSMode) {
        vfsMode.withVFS { vfs ->
            val walFile = vfs.file("test.wal")

            val tx1Id = UUID.fromString("d5215e86-d787-4c50-8dea-a8da76771a03")
            val tx2Id = UUID.fromString("60fe702f-c0f2-44b6-8216-d67a6256d4ed")
            val tx3Id = UUID.fromString("996c8fb9-b659-4307-b574-36e7a3c21bd0")

            val storeId = UUID.randomUUID()

            walFile.withOverWriter { overWriter ->
                val outputStream = overWriter.outputStream
                WriteAheadLogEntry.beginTransaction(tx1Id).writeTo(outputStream)
                WriteAheadLogEntry.put(tx1Id, storeId, BasicBytes("foo"), BasicBytes("bar")).writeTo(outputStream)
                WriteAheadLogEntry.put(tx1Id, storeId, BasicBytes("hello"), BasicBytes("world")).writeTo(outputStream)

                WriteAheadLogEntry.beginTransaction(tx2Id).writeTo(outputStream)
                WriteAheadLogEntry.commit(tx1Id, 123456, Bytes.EMPTY).writeTo(outputStream)

                WriteAheadLogEntry.put(tx2Id, storeId, BasicBytes("foo"), BasicBytes("baz")).writeTo(outputStream)
                WriteAheadLogEntry.commit(tx2Id, 123456, Bytes.EMPTY).writeTo(outputStream)

                WriteAheadLogEntry.beginTransaction(tx3Id).writeTo(outputStream)
                WriteAheadLogEntry.put(tx3Id, storeId, BasicBytes("lorem"), BasicBytes("ipsum")).writeTo(outputStream)
                WriteAheadLogEntry.commit(tx3Id, 777777, Bytes.EMPTY).writeTo(outputStream)

                overWriter.commit()
            }

            val wal = WriteAheadLog(walFile)
            wal.performStartupRecoveryCleanup()

            val transactions = wal.allTransactions

            expectThat(transactions).hasSize(2).and {
                get(0).and {
                    get { this.transactionId }.isEqualTo(tx2Id)
                    get { this.commitMetadata }.isEqualTo(Bytes.EMPTY)
                    get { this.storeIdToCommands.entries }.single().and {
                        get { this.key }.isEqualTo(storeId)
                        get { this.value }.single().and {
                            get { this.key }.isEqualTo(BasicBytes("foo"))
                            get { this.value }.isEqualTo(BasicBytes("baz"))
                            get { this.opCode }.isEqualTo(Command.OpCode.PUT)
                            get { this.timestamp }.isEqualTo(123456)
                        }
                    }
                }
                get(1).and {
                    get { this.transactionId }.isEqualTo(tx3Id)
                    get { this.commitMetadata }.isEqualTo(Bytes.EMPTY)
                    get { this.storeIdToCommands.entries }.single().and {
                        get { this.key }.isEqualTo(storeId)
                        get { this.value }.single().and {
                            get { this.key }.isEqualTo(BasicBytes("lorem"))
                            get { this.value }.isEqualTo(BasicBytes("ipsum"))
                            get { this.opCode }.isEqualTo(Command.OpCode.PUT)
                            get { this.timestamp }.isEqualTo(777777)
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

            val tx1Id = UUID.fromString("d5215e86-d787-4c50-8dea-a8da76771a03")
            val tx2Id = UUID.fromString("60fe702f-c0f2-44b6-8216-d67a6256d4ed")
            val tx3Id = UUID.fromString("996c8fb9-b659-4307-b574-36e7a3c21bd0")

            val storeId = UUID.randomUUID()

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


}