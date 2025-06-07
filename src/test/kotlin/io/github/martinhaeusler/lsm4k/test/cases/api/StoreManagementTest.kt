package io.github.martinhaeusler.lsm4k.test.cases.api

import io.github.martinhaeusler.lsm4k.api.LSM4KConfiguration
import io.github.martinhaeusler.lsm4k.api.LSM4KTransaction
import io.github.martinhaeusler.lsm4k.api.TransactionalStore.Companion.withCursor
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualDirectory
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFile
import io.github.martinhaeusler.lsm4k.lsm.LSMTreeFile
import io.github.martinhaeusler.lsm4k.manifest.ManifestFile
import io.github.martinhaeusler.lsm4k.test.extensions.transaction.LSM4KTransactionTestExtensions.allEntries
import io.github.martinhaeusler.lsm4k.test.extensions.transaction.LSM4KTransactionTestExtensions.delete
import io.github.martinhaeusler.lsm4k.test.extensions.transaction.LSM4KTransactionTestExtensions.get
import io.github.martinhaeusler.lsm4k.test.extensions.transaction.LSM4KTransactionTestExtensions.put
import io.github.martinhaeusler.lsm4k.test.util.DatabaseEngineTest
import io.github.martinhaeusler.lsm4k.test.util.LSM4KMode
import io.github.martinhaeusler.lsm4k.test.util.junit.IntegrationTest
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.bytes.BasicBytes
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.GiB
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.fail
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*
import kotlin.math.min

@IntegrationTest
class StoreManagementTest {

    @DatabaseEngineTest
    fun canCreateStoresAndPerformPutAndGet(mode: LSM4KMode) {
        mode.withDatabaseEngine { engine ->
            engine.readWriteTransaction { tx ->
                tx.createNewStore("test")
                tx.createNewStore("math")
                tx.commit()
            }
            engine.readWriteTransaction { tx ->
                val test = tx.getStore("test")
                test.put("foo", "bar")

                val math = tx.getStore("math")
                math.put("pi", "3.1415")
                math.put("e", "2.718")

                tx.commit()
            }
            engine.readWriteTransaction { tx ->
                expectThat(tx) {
                    get { allStores }.hasSize(2).and {
                        any {
                            get { this.storeId }.isEqualTo(StoreId.of("test"))
                            get { this.get("foo") }.isEqualTo(BasicBytes("bar"))
                            get { this.get("bullshit") }.isNull()
                        }
                        any {
                            get { this.storeId }.isEqualTo(StoreId.of("math"))
                            get { this.get("pi") }.isEqualTo(BasicBytes("3.1415"))
                            get { this.get("e") }.isEqualTo(BasicBytes("2.718"))
                        }
                    }

                }
            }
        }
    }

    @DatabaseEngineTest
    fun canCreateStoresAndPerformPutAndGetInSameTransaction(mode: LSM4KMode) {

        fun performAssertions(tx: LSM4KTransaction) {
            expectThat(tx) {
                get { allStores }.hasSize(2).and {
                    any {
                        get { this.storeId }.isEqualTo(StoreId.of("test"))
                        get { this.get("foo") }.isEqualTo(BasicBytes("bar"))
                        get { this.get("bullshit") }.isNull()
                    }
                    any {
                        get { this.storeId }.isEqualTo(StoreId.of("math"))
                        get { this.get("pi") }.isEqualTo(BasicBytes("3.1415"))
                        get { this.get("e") }.isEqualTo(BasicBytes("2.718"))
                    }
                }
            }
        }

        mode.withDatabaseEngine { engine ->
            engine.readWriteTransaction { tx ->
                val test = tx.createNewStore("test")
                test.put("foo", "bar")
                val math = tx.createNewStore("math")
                math.put("pi", "3.1415")
                math.put("e", "2.718")

                performAssertions(tx)

                tx.commit()
            }
            engine.readWriteTransaction { tx ->
                performAssertions(tx)
            }
        }
    }

    @DatabaseEngineTest
    fun canCreateStoreAndIterateWithCursor(mode: LSM4KMode) {

        fun performAssertions(tx: LSM4KTransaction) {
            expectThat(tx) {
                get { allStores }.hasSize(2).and {
                    any {
                        get { this.storeId }.isEqualTo(StoreId.of("test"))
                        get { this.allEntries }.containsExactly(BasicBytes("foo") to BasicBytes("bar"))
                    }
                    any {
                        get { this.storeId }.isEqualTo(StoreId.of("math"))
                        get { this.allEntries }.containsExactly(
                            BasicBytes("e") to BasicBytes("2.718"),
                            BasicBytes("pi") to BasicBytes("3.1415")
                        )
                    }
                }
            }
        }

        mode.withDatabaseEngineAndVFS { engine, vfs ->
            engine.readWriteTransaction { tx ->
                val test = tx.createNewStore("test")
                test.put("foo", "bar")
                val math = tx.createNewStore("math")
                math.put("pi", "3.1415")
                math.put("e", "2.718")

                performAssertions(tx)

                tx.commit()
            }

            engine.readWriteTransaction { tx ->
                performAssertions(tx)
            }

            expectThat(vfs.listRootLevelElements()) {
                filterIsInstance<VirtualDirectory>().filter { it.name == "test" }.hasSize(1)
                filterIsInstance<VirtualDirectory>().filter { it.name == "math" }.hasSize(1)
                filterIsInstance<VirtualFile>().filter { it.name == ManifestFile.FILE_NAME }.hasSize(1)
            }
        }
    }

    @DatabaseEngineTest
    fun canCompactWhileIterationIsOngoing(mode: LSM4KMode) {
        val numberOfEntries = 10_000

        fun assertListEntries(list: List<Pair<Bytes, Bytes>>) {
            for (index in 0 until min(list.size, numberOfEntries)) {
                val entry = list[index]
                Assertions.assertEquals(createKey(index), entry.first.asString())
                Assertions.assertEquals("value#${index}", entry.second.asString())
            }
            if (list.size != numberOfEntries) {
                fail("Expected result list to have size ${numberOfEntries}, but found ${list.size}! The existing entries have the expected structure and ordering. The last key is: ${list.lastOrNull()?.first?.asString() ?: "<null>"}")
            }
        }

        val config = LSM4KConfiguration(
            // disable flushing and merging, we will do it manually in this test
            maxForestSize = 10.GiB,
            minorCompactionCron = null,
            majorCompactionCron = null,
        )
        mode.withDatabaseEngine(config) { engine ->
            engine.readWriteTransaction { tx ->
                val data = tx.createNewStore("data")
                repeat(numberOfEntries) { i ->
                    data.put(createKey(i), "value#${i}")
                }
                tx.commit()
            }

            // start iterating in a new transaction
            val tx1 = engine.beginReadWriteTransaction()
            val c1 = tx1.getStore("data").openCursor()
            c1.firstOrThrow()
            val c1Sequence1 = c1.ascendingEntrySequenceFromHere()

            // flush the in-memory stores to the VFS
            engine.flushAllStoresSynchronous()

            val c1Result = c1Sequence1.toList()
            assertListEntries(c1Result)

            c1.firstOrThrow()

            val tx2 = engine.beginReadWriteTransaction()
            val c2 = tx2.getStore("data").openCursor()

            c2.firstOrThrow()
            val c2Sequence1 = c2.ascendingEntrySequenceFromHere()
            val c1Sequence2 = c1.ascendingEntrySequenceFromHere()

            engine.majorCompactionOnAllStoresSynchronous()

            assertListEntries(c1Sequence2.toList())

            assertListEntries(c2Sequence1.toList())

            c1.close()
            tx1.close()
            c2.close()
            tx2.close()
        }
    }

    @DatabaseEngineTest
    fun compactionDoesNotAffectRepeatableReads(mode: LSM4KMode) {
        val config = LSM4KConfiguration(
            // disable auto-compaction, we will compact manually
            minorCompactionCron = null,
            majorCompactionCron = null,
        )

        mode.withDatabaseEngine(config) { engine ->
            engine.readWriteTransaction { tx ->
                tx.createNewStore("test")
                tx.commit()
            }
            // write some data, flush after every transaction
            repeat(10) { txIndex ->
                engine.readWriteTransaction { tx ->
                    val store = tx.getStore("test")
                    repeat(1000) { i ->
                        store.put("key#${i}", "value#${i}@${txIndex}")
                    }
                    tx.commit()
                }
                engine.flushAllStoresSynchronous()
            }

            // now we should have 10 files on disk.
            engine.readWriteTransaction { readTx ->
                // grab all the latest entries from the store
                val entryMap = readTx.getStore("test").withCursor { cursor ->
                    cursor.firstOrThrow()
                    cursor.ascendingEntrySequenceFromHere().toMap()
                }

                // have a parallel transaction commit newer data
                engine.readWriteTransaction { writeTx ->
                    val store = writeTx.getStore("test")
                    repeat(1000) { i ->
                        store.put("key#${i}", "value#${i}@${11}")
                    }
                    writeTx.commit()
                }

                engine.flushAllStoresSynchronous()

                engine.majorCompactionOnAllStoresSynchronous()

                // grab the entries again on our still-open transaction
                val entryMap2 = readTx.getStore("test").withCursor { cursor ->
                    cursor.firstOrThrow()
                    cursor.ascendingEntrySequenceFromHere().toMap()
                }

                // ensure that repeatable reads isolation was not violated
                expectThat(entryMap2.entries).containsExactlyInAnyOrder(entryMap.entries)
            }
        }

    }

    @DatabaseEngineTest
    fun canIterateOverAllVersionsWithMultipleFiles(mode: LSM4KMode) {
        val config = LSM4KConfiguration(
            // disable flushing and merging, we will do it manually in this test
            maxForestSize = 10.GiB,
            minorCompactionCron = null,
            majorCompactionCron = null,
        )

        val numberOfEntries = 20
        mode.withDatabaseEngine(config) { engine, vfs ->
            //
            // We're creating the following situation:
            //
            // | KEY  | Commit 1 | Commit 2 | Commit 3 | Commit 4 |
            // |------|----------|----------|----------|----------|
            // | 0    | a        | b        | -        | c        |
            // | 1    | a        | a        | a        | a        |
            // | 2    | a        | a        | a        | a        |
            // | 3    | a        | b        | b        | b        |
            // | 4    | a        | a        | a        | a        |
            // | 5    | a        | a        | -        | -        |
            // | 6    | a        | b        | b        | b        |
            // | 7    | a        | a        | a        | c        |
            // | 8    | a        | a        | a        | a        |
            // | 9    | a        | b        | b        | b        |
            // | 10   | a        | a        | -        | -        |
            // | 11   | a        | a        | a        | a        |
            // | 12   | a        | b        | b        | b        |
            // | 13   | a        | a        | a        | a        |
            // | 14   | a        | a        | a        | c        |
            // | 15   | a        | b        | -        | -        |
            // | 16   | a        | a        | a        | a        |
            // | 17   | a        | a        | a        | a        |
            // | 18   | a        | b        | b        | b        |
            // | 19   | a        | a        | a        | a        |
            //
            val txAtZero = engine.beginReadWriteTransaction()


            engine.readWriteTransaction { tx ->
                val data = tx.createNewStore("data")
                repeat(numberOfEntries) { i ->
                    data.put(createKey(i), "a")
                }
                tx.commit()
            }

            val txAtTSN1 = engine.beginReadWriteTransaction()

            engine.readWriteTransaction { tx ->
                val data = tx.getStore("data")
                for (i in (0..<numberOfEntries step 3)) {
                    data.put(createKey(i), "b")
                }
                tx.commit()
            }

            val txAtTSN2 = engine.beginReadWriteTransaction()

            // flush the in-memory stores to the VFS
            engine.forest.flushAllInMemoryStoresToDisk()

            engine.readWriteTransaction { tx ->
                val data = tx.getStore("data")
                for (i in (0..<numberOfEntries step 5)) {
                    data.delete(createKey(i))
                }
                tx.commit()
            }

            val txAtTSN3 = engine.beginReadWriteTransaction()

            // flush the in-memory stores to the VFS
            engine.forest.flushAllInMemoryStoresToDisk()

            engine.readWriteTransaction { tx ->
                val data = tx.getStore("data")
                for (i in (0..<numberOfEntries step 7)) {
                    data.put(createKey(i), "c")
                }
                tx.commit()
            }

            val txAtTSN4 = engine.beginReadWriteTransaction()

            val storeDir = vfs.directory("data")

            // since we've performed two flushes, we should have two files in the VFS.
            expectThat(storeDir)
                .get { this.list() }
                .filter { it.endsWith(LSMTreeFile.FILE_EXTENSION) }
                .hasSize(2)

            // iterate over the data.
            engine.readWriteTransaction { tx ->
                tx.getStore("data").withCursor { cursor ->
                    cursor.firstOrThrow()
                    val entries = cursor.ascendingEntrySequenceFromHere().toList()
                    expectThat(entries).map { it.first.asString() to it.second.asString() }.containsExactly(
                        createKey(0) to "c",
                        createKey(1) to "a",
                        createKey(2) to "a",
                        createKey(3) to "b",
                        createKey(4) to "a",
                        createKey(6) to "b",
                        createKey(7) to "c",
                        createKey(8) to "a",
                        createKey(9) to "b",
                        createKey(11) to "a",
                        createKey(12) to "b",
                        createKey(13) to "a",
                        createKey(14) to "c",
                        createKey(16) to "a",
                        createKey(17) to "a",
                        createKey(18) to "b",
                        createKey(19) to "a",
                    )
                }

                expectThat(txAtZero.getStoreOrNull("data")).isNull()


                txAtTSN1.getStore("data").withCursor { cursor ->
                    cursor.firstOrThrow()
                    val entries = cursor.ascendingEntrySequenceFromHere().toList()
                    expectThat(entries).map { it.first.asString() to it.second.asString() }.containsExactly(
                        createKey(0) to "a",
                        createKey(1) to "a",
                        createKey(2) to "a",
                        createKey(3) to "a",
                        createKey(4) to "a",
                        createKey(5) to "a",
                        createKey(6) to "a",
                        createKey(7) to "a",
                        createKey(8) to "a",
                        createKey(9) to "a",
                        createKey(10) to "a",
                        createKey(11) to "a",
                        createKey(12) to "a",
                        createKey(13) to "a",
                        createKey(14) to "a",
                        createKey(15) to "a",
                        createKey(16) to "a",
                        createKey(17) to "a",
                        createKey(18) to "a",
                        createKey(19) to "a",
                    )
                }

                txAtTSN2.getStore("data").withCursor { cursor ->
                    cursor.firstOrThrow()
                    val entries = cursor.ascendingEntrySequenceFromHere().toList()
                    expectThat(entries).map { it.first.asString() to it.second.asString() }.containsExactly(
                        createKey(0) to "b",
                        createKey(1) to "a",
                        createKey(2) to "a",
                        createKey(3) to "b",
                        createKey(4) to "a",
                        createKey(5) to "a",
                        createKey(6) to "b",
                        createKey(7) to "a",
                        createKey(8) to "a",
                        createKey(9) to "b",
                        createKey(10) to "a",
                        createKey(11) to "a",
                        createKey(12) to "b",
                        createKey(13) to "a",
                        createKey(14) to "a",
                        createKey(15) to "b",
                        createKey(16) to "a",
                        createKey(17) to "a",
                        createKey(18) to "b",
                        createKey(19) to "a",
                    )
                }

                txAtTSN3.getStore("data").withCursor { cursor ->
                    cursor.firstOrThrow()
                    val entries = cursor.ascendingEntrySequenceFromHere().toList()
                    expectThat(entries).map { it.first.asString() to it.second.asString() }.containsExactly(
                        createKey(1) to "a",
                        createKey(2) to "a",
                        createKey(3) to "b",
                        createKey(4) to "a",
                        createKey(6) to "b",
                        createKey(7) to "a",
                        createKey(8) to "a",
                        createKey(9) to "b",
                        createKey(11) to "a",
                        createKey(12) to "b",
                        createKey(13) to "a",
                        createKey(14) to "a",
                        createKey(16) to "a",
                        createKey(17) to "a",
                        createKey(18) to "b",
                        createKey(19) to "a",
                    )
                }

                txAtTSN4.getStore("data").withCursor { cursor ->
                    cursor.firstOrThrow()
                    val entries = cursor.ascendingEntrySequenceFromHere().toList()
                    expectThat(entries).map { it.first.asString() to it.second.asString() }.containsExactly(
                        createKey(0) to "c",
                        createKey(1) to "a",
                        createKey(2) to "a",
                        createKey(3) to "b",
                        createKey(4) to "a",
                        createKey(6) to "b",
                        createKey(7) to "c",
                        createKey(8) to "a",
                        createKey(9) to "b",
                        createKey(11) to "a",
                        createKey(12) to "b",
                        createKey(13) to "a",
                        createKey(14) to "c",
                        createKey(16) to "a",
                        createKey(17) to "a",
                        createKey(18) to "b",
                        createKey(19) to "a",
                    )
                }
            }
        }
    }

    @DatabaseEngineTest
    fun canIsolateTransactionsFromOneAnother(mode: LSM4KMode) {
        mode.withDatabaseEngine { engine ->
            engine.readWriteTransaction { tx ->
                // TSN start at 1
                expectThat(tx.lastVisibleSerialNumber).isEqualTo(1)

                val data = tx.createNewStore("data")
                // creating a store increases the TSN (now at 2)
                data.put("hello", "world")
                data.put("john", "doe")
                val math = tx.createNewStore("math")
                // creating a store increases the TSN (now at 3)
                math.put("pi", "3.1415")
                math.put("e", "2.7182")

                // committing a transaction increases the TSN (now at 4)
                tx.commit()
            }

            engine.readWriteTransaction { tx1 ->
                engine.readWriteTransaction { tx2 ->

                    expect {
                        that(tx1.lastVisibleSerialNumber).isEqualTo(4)
                        that(tx2.lastVisibleSerialNumber).isEqualTo(4)
                    }

                    tx1.getStore("data").put("foo", "bar")
                    tx1.getStore("data").delete("john")
                    tx1.getStore("math").put("zero", "0")
                    tx1.getStore("math").delete("pi")

                    // this commit writes to TSN 5
                    val writeTSN = tx1.commit()

                    expectThat(writeTSN).isEqualTo(5)

                    // tx2 should not see the changes performed by tx1
                    expectThat(tx2) {
                        get { this.getStore("data").getEntriesAscending().asStrings() }.containsExactly(
                            "hello" to "world",
                            "john" to "doe"
                        )

                        get { this.getStore("math").getEntriesAscending().asStrings() }.containsExactly(
                            "e" to "2.7182",
                            "pi" to "3.1415"
                        )
                    }
                }
            }
        }
    }

    @DatabaseEngineTest
    fun cannotCreateNestedStoresInSameTransaction(mode: LSM4KMode) {
        mode.withDatabaseEngine { engine ->
            engine.readWriteTransaction { tx ->
                // ok, since we don't have any stores yet.
                tx.createNewStore("foo/bar")
                // ok, since "foo" itself is not a store.
                tx.createNewStore("foo/baz")
                // not ok, since "foo/bar" and "foo/baz" are stores
                expectThrows<IllegalArgumentException> {
                    tx.createNewStore("foo")
                }
                // not ok, since "foo/bar" is already a store
                expectThrows<IllegalArgumentException> {
                    tx.createNewStore("foo/bar/baz")
                }
                tx.commit()
            }

            engine.readWriteTransaction { tx ->
                expectThat(tx.allStores).map { it.storeId.toString() }.containsExactly("foo/bar", "foo/baz")
            }
        }
    }

    @DatabaseEngineTest
    fun cannotCreateNestedStoresInDifferentTransaction(mode: LSM4KMode) {
        mode.withDatabaseEngine { engine ->
            engine.readWriteTransaction { tx ->
                // ok, since we don't have any stores yet.
                tx.createNewStore("foo/bar")
                // ok, since "foo" itself is not a store.
                tx.createNewStore("foo/baz")
                tx.commit()
            }

            engine.readWriteTransaction { tx ->
                // not ok, since "foo/bar" and "foo/baz" are stores
                expectThrows<IllegalArgumentException> {
                    tx.createNewStore("foo")
                }
                // not ok, since "foo/bar" is already a store
                expectThrows<IllegalArgumentException> {
                    tx.createNewStore("foo/bar/baz")
                }
                // this commit will be empty!
                tx.commit()
            }

            engine.readWriteTransaction { tx ->
                expectThat(tx.allStores).map { it.storeId.toString() }.containsExactly("foo/bar", "foo/baz")
            }
        }
    }

    private fun createKey(key: Int): String {
        return "key#${key.toString().padStart(6, '0')}"
    }

    private fun List<Pair<Bytes, Bytes>>.asStrings(): List<Pair<String, String>> {
        return this.map { it.first.asString() to it.second.asString() }
    }
}
