package org.chronos.chronostore.test.cases.api

import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.TransactionalStore.Companion.withCursor
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.lsm.LSMTreeFile
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.allEntries
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.delete
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.get
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.put
import org.chronos.chronostore.test.util.ChronoStoreMode
import org.chronos.chronostore.test.util.ChronoStoreTest
import org.chronos.chronostore.test.util.junit.IntegrationTest
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.bytes.BasicBytes
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.unit.BinarySize.Companion.GiB
import org.chronos.chronostore.util.unit.BinarySize.Companion.MiB
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.fail
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*
import kotlin.math.min

@IntegrationTest
class StoreManagementTest {

    @ChronoStoreTest
    fun canCreateStoresAndPerformPutAndGet(mode: ChronoStoreMode) {
        mode.withChronoStore { chronoStore ->
            chronoStore.transaction { tx ->
                tx.createNewStore("test")
                tx.createNewStore("math")
                tx.commit()
            }
            chronoStore.transaction { tx ->
                val test = tx.getStore("test")
                test.put("foo", "bar")

                val math = tx.getStore("math")
                math.put("pi", "3.1415")
                math.put("e", "2.718")

                tx.commit()
            }
            chronoStore.transaction { tx ->
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

    @ChronoStoreTest
    fun canCreateStoresAndPerformPutAndGetInSameTransaction(mode: ChronoStoreMode) {

        fun performAssertions(tx: ChronoStoreTransaction) {
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

        mode.withChronoStore { chronoStore ->
            chronoStore.transaction { tx ->
                val test = tx.createNewStore("test")
                test.put("foo", "bar")
                val math = tx.createNewStore("math")
                math.put("pi", "3.1415")
                math.put("e", "2.718")

                performAssertions(tx)

                tx.commit()
            }
            chronoStore.transaction { tx ->
                performAssertions(tx)
            }
        }
    }

    @ChronoStoreTest
    fun canCreateStoreAndIterateWithCursor(mode: ChronoStoreMode) {

        fun performAssertions(tx: ChronoStoreTransaction) {
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

        mode.withChronoStoreAndVFS { chronoStore, vfs ->
            chronoStore.transaction { tx ->
                val test = tx.createNewStore("test")
                test.put("foo", "bar")
                val math = tx.createNewStore("math")
                math.put("pi", "3.1415")
                math.put("e", "2.718")

                performAssertions(tx)

                tx.commit()
            }

            chronoStore.transaction { tx ->
                performAssertions(tx)
            }

            expectThat(vfs.listRootLevelElements()) {
                filterIsInstance<VirtualDirectory>().filter { it.name == "test" }.hasSize(1)
                filterIsInstance<VirtualDirectory>().filter { it.name == "math" }.hasSize(1)
                filterIsInstance<VirtualFile>().filter { it.name == ManifestFile.FILE_NAME }.hasSize(1)
            }
        }
    }

    @ChronoStoreTest
    fun canCompactWhileIterationIsOngoing(mode: ChronoStoreMode) {
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

        val config = ChronoStoreConfiguration(
            // disable flushing and merging, we will do it manually in this test
            maxForestSize = 10.GiB,
            compactionInterval = null,
        )
        mode.withChronoStore(config) { chronoStore ->
            chronoStore.transaction { tx ->
                val data = tx.createNewStore("data")
                repeat(numberOfEntries) { i ->
                    data.put(createKey(i), "value#${i}")
                }
                tx.commit()
            }

            // start iterating in a new transaction
            val tx1 = chronoStore.beginTransaction()
            val c1 = tx1.getStore("data").openCursor()
            c1.firstOrThrow()
            val c1Sequence1 = c1.ascendingEntrySequenceFromHere()

            // flush the in-memory stores to the VFS
            chronoStore.mergeService.flushAllInMemoryStoresToDisk()

            val c1Result = c1Sequence1.toList()
            assertListEntries(c1Result)

            c1.firstOrThrow()

            val tx2 = chronoStore.beginTransaction()
            val c2 = tx2.getStore("data").openCursor()

            c2.firstOrThrow()
            val c2Sequence1 = c2.ascendingEntrySequenceFromHere()
            val c1Sequence2 = c1.ascendingEntrySequenceFromHere()

            chronoStore.mergeService.performMajorCompaction()

            assertListEntries(c1Sequence2.toList())

            assertListEntries(c2Sequence1.toList())

            c1.close()
            tx1.close()
            c2.close()
            tx2.close()
        }
    }

    @ChronoStoreTest
    fun compactionDoesNotAffectRepeatableReads(mode: ChronoStoreMode){
        val config = ChronoStoreConfiguration(
            // disable auto-compaction, we will compact manually
            compactionInterval = null,
        )

        mode.withChronoStore(config) { chronoStore ->
            chronoStore.transaction { tx ->
                tx.createNewStore("test")
                tx.commit()
            }
            // write some data, flush after every transaction
            repeat(10){ txIndex ->
                chronoStore.transaction { tx ->
                    val store = tx.getStore("test")
                    repeat(1000){ i ->
                        store.put("key#${i}", "value#${i}@${txIndex}")
                    }
                    tx.commit()
                }
                chronoStore.mergeService.flushAllInMemoryStoresToDisk()
            }

            // now we should have 10 files on disk.
            chronoStore.transaction { readTx ->
                // grab all the latest entries from the store
                val entryMap = readTx.getStore("test").withCursor { cursor ->
                    cursor.firstOrThrow()
                    cursor.ascendingEntrySequenceFromHere().toMap()
                }

                // have a parallel transaction commit newer data
                chronoStore.transaction { writeTx ->
                    val store = writeTx.getStore("test")
                    repeat(1000){ i ->
                        store.put("key#${i}", "value#${i}@${11}")
                    }
                    writeTx.commit()
                }

                chronoStore.mergeService.flushAllInMemoryStoresToDisk()

                chronoStore.mergeService.performMajorCompaction()

                // grab the entries again on our still-open transaction
                val entryMap2 = readTx.getStore("test").withCursor { cursor ->
                    cursor.firstOrThrow()
                    cursor.ascendingEntrySequenceFromHere().toMap()
                }

                // ensure that repeatable reads isolation was not violated
                expectThat(entryMap.entries).containsExactlyInAnyOrder(entryMap2.entries)
            }
        }

    }

    @ChronoStoreTest
    fun canIterateOverAllVersionsWithMultipleFiles(mode: ChronoStoreMode) {
        val config = ChronoStoreConfiguration(
            // disable flushing and merging, we will do it manually in this test
            maxForestSize = 10.GiB,
            compactionInterval = null,
        )

        val numberOfEntries = 20
        mode.withChronoStore(config) { chronoStore, vfs ->
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
            val txAtZero = chronoStore.beginTransaction()


            chronoStore.transaction { tx ->
                val data = tx.createNewStore("data")
                repeat(numberOfEntries) { i ->
                    data.put(createKey(i), "a")
                }
                tx.commit()
            }

            val txAtTSN1 = chronoStore.beginTransaction()

            chronoStore.transaction { tx ->
                val data = tx.getStore("data")
                for (i in (0..<numberOfEntries step 3)) {
                    data.put(createKey(i), "b")
                }
                tx.commit()
            }

            val txAtTSN2 = chronoStore.beginTransaction()

            // flush the in-memory stores to the VFS
            chronoStore.forest.flushAllInMemoryStoresToDisk()

            chronoStore.transaction { tx ->
                val data = tx.getStore("data")
                for (i in (0..<numberOfEntries step 5)) {
                    data.delete(createKey(i))
                }
                tx.commit()
            }

            val txAtTSN3 = chronoStore.beginTransaction()

            // flush the in-memory stores to the VFS
            chronoStore.forest.flushAllInMemoryStoresToDisk()

            chronoStore.transaction { tx ->
                val data = tx.getStore("data")
                for (i in (0..<numberOfEntries step 7)) {
                    data.put(createKey(i), "c")
                }
                tx.commit()
            }

            val txAtTSN4 = chronoStore.beginTransaction()

            val storeDir = vfs.directory("data")

            // since we've performed two flushes, we should have two files in the VFS.
            expectThat(storeDir)
                .get { this.list() }
                .filter { it.endsWith(LSMTreeFile.FILE_EXTENSION) }
                .hasSize(2)

            // iterate over the data.
            chronoStore.transaction { tx ->
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

    @ChronoStoreTest
    fun canIsolateTransactionsFromOneAnother(mode: ChronoStoreMode) {
        mode.withChronoStore { chronoStore ->
            chronoStore.transaction { tx ->
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

            chronoStore.transaction { tx1 ->
                chronoStore.transaction { tx2 ->

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

    @ChronoStoreTest
    fun cannotCreateNestedStoresInSameTransaction(mode: ChronoStoreMode) {
        mode.withChronoStore { chronoStore ->
            chronoStore.transaction { tx ->
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

            chronoStore.transaction { tx ->
                expectThat(tx.allStores).map { it.storeId.toString() }.containsExactly("foo/bar", "foo/baz")
            }
        }
    }

    @ChronoStoreTest
    fun cannotCreateNestedStoresInDifferentTransaction(mode: ChronoStoreMode) {
        mode.withChronoStore { chronoStore ->
            chronoStore.transaction { tx ->
                // ok, since we don't have any stores yet.
                tx.createNewStore("foo/bar")
                // ok, since "foo" itself is not a store.
                tx.createNewStore("foo/baz")
                tx.commit()
            }

            chronoStore.transaction { tx ->
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

            chronoStore.transaction { tx ->
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
