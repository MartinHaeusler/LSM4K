package org.chronos.chronostore.test.cases.wal

import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.test.util.VFSMode
import org.chronos.chronostore.test.util.VirtualFileSystemTest
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.wal.WriteAheadLog
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
                        Command.put(Bytes("hello"), 1000, Bytes("world")),
                        Command.put(Bytes("foo"), 1000, Bytes("bar")),
                    ),
                    store2Id to listOf(
                        Command.put(Bytes("pi"), 1000, Bytes("3.1415")),
                        Command.put(Bytes("e"), 1000, Bytes("2.71828"))
                    ),
                    store3Id to listOf(
                        Command.put(Bytes("john"), 1000, Bytes("doe")),
                        Command.put(Bytes("jane"), 1000, Bytes("doe")),
                    )
                ),
                commitMetadata = Bytes("John was here")
            )

            val tx2 = WriteAheadLogTransaction(
                transactionId = tx2Id,
                commitTimestamp = 1234,
                storeIdToCommands = emptyMap(),
                commitMetadata = Bytes("Empty Commit")
            )


            val tx3 = WriteAheadLogTransaction(
                transactionId = tx3Id,
                commitTimestamp = 777777,
                storeIdToCommands = mapOf(
                    store1Id to listOf(
                        Command.del(Bytes("hello"), 777777),
                        Command.put(Bytes("foo"), 777777, Bytes("baz")),
                    ),
                    store3Id to listOf(
                        Command.put(Bytes("sarah"), 777777, Bytes("doe")),
                    )
                ),
                commitMetadata = Bytes("Jane did this")
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
                    get { this.commitMetadata }.isEqualTo(Bytes("John was here"))
                    get { this.storeIdToCommands }.hasSize(3).and {
                        get(store1Id).isNotNull().hasSize(2).and {
                            get(0).isEqualTo(Command.put(Bytes("hello"), 1000, Bytes("world")))
                            get(1).isEqualTo(Command.put(Bytes("foo"), 1000, Bytes("bar")))
                        }
                        get(store2Id).isNotNull().hasSize(2).and {
                            get(0).isEqualTo(Command.put(Bytes("pi"), 1000, Bytes("3.1415")))
                            get(1).isEqualTo(Command.put(Bytes("e"), 1000, Bytes("2.71828")))
                        }
                        get(store3Id).isNotNull().hasSize(2).and {
                            get(0).isEqualTo(Command.put(Bytes("john"), 1000, Bytes("doe")))
                            get(1).isEqualTo(Command.put(Bytes("jane"), 1000, Bytes("doe")))
                        }
                    }
                }
                get(1).and {
                    get { this.transactionId }.isEqualTo(tx2Id)
                    get { this.commitTimestamp }.isEqualTo(1234)
                    get { this.commitMetadata }.isEqualTo(Bytes("Empty Commit"))
                    get { this.storeIdToCommands }.isEmpty()
                }
                get(2).and {
                    get { this.transactionId }.isEqualTo(tx3Id)
                    get { this.commitTimestamp }.isEqualTo(777777)
                    get { this.commitMetadata }.isEqualTo(Bytes("Jane did this"))
                    get { this.storeIdToCommands }.hasSize(2).and {
                        get(store1Id).isNotNull().hasSize(2).and {
                            get(0).isEqualTo(Command.del(Bytes("hello"), 777777))
                            get(1).isEqualTo(Command.put(Bytes("foo"), 777777, Bytes("baz")))
                        }
                        get(store3Id).isNotNull().hasSize(1).and {
                            get(0).isEqualTo(Command.put(Bytes("sarah"), 777777, Bytes("doe")))
                        }
                    }
                }
            }
        }
    }

}