package org.chronos.chronostore.test.cases.api

import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.lsm.LSMTreeFile
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.allEntriesOnLatest
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.delete
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.getLatest
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.put
import org.chronos.chronostore.test.util.ChronoStoreMode
import org.chronos.chronostore.test.util.ChronoStoreTest
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.bytes.BasicBytes
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.unit.GiB
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.fail
import strikt.api.expectThat
import strikt.assertions.*
import kotlin.math.min
import kotlin.time.Duration.Companion.minutes

class StoreManagementTest {

    @ChronoStoreTest
    fun canCreateStoresAndPerformPutAndGet(mode: ChronoStoreMode) {
        mode.withChronoStore { chronoStore ->
            chronoStore.transaction { tx ->
                tx.createNewStore("test", versioned = true)
                tx.createNewStore("math", versioned = false)
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
                            get { this.store.name }.isEqualTo(StoreId.of("test"))
                            get { this.getLatest("foo") }.isEqualTo(BasicBytes("bar"))
                            get { this.getLatest("bullshit") }.isNull()
                        }
                        any {
                            get { this.store.name }.isEqualTo(StoreId.of("math"))
                            get { this.getLatest("pi") }.isEqualTo(BasicBytes("3.1415"))
                            get { this.getLatest("e") }.isEqualTo(BasicBytes("2.718"))
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
                        get { this.store.name }.isEqualTo(StoreId.of("test"))
                        get { this.getLatest("foo") }.isEqualTo(BasicBytes("bar"))
                        get { this.getLatest("bullshit") }.isNull()
                    }
                    any {
                        get { this.store.name }.isEqualTo(StoreId.of("math"))
                        get { this.getLatest("pi") }.isEqualTo(BasicBytes("3.1415"))
                        get { this.getLatest("e") }.isEqualTo(BasicBytes("2.718"))
                    }
                }
            }
        }

        mode.withChronoStore { chronoStore ->
            chronoStore.transaction { tx ->
                val test = tx.createNewStore("test", versioned = true)
                test.put("foo", "bar")
                val math = tx.createNewStore("math", versioned = false)
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
                        get { this.store.name }.isEqualTo(StoreId.of("test"))
                        get { this.allEntriesOnLatest }.containsExactly(BasicBytes("foo") to BasicBytes("bar"))
                    }
                    any {
                        get { this.store.name }.isEqualTo(StoreId.of("math"))
                        get { this.allEntriesOnLatest }.containsExactly(
                            BasicBytes("e") to BasicBytes("2.718"),
                            BasicBytes("pi") to BasicBytes("3.1415")
                        )
                    }
                }
            }
        }

        mode.withChronoStore { chronoStore ->
            chronoStore.transaction { tx ->
                val test = tx.createNewStore("test", versioned = true)
                test.put("foo", "bar")
                val math = tx.createNewStore("math", versioned = false)
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

        val config = ChronoStoreConfiguration()
        // disable flushing and merging, we will do it manually in this test
        config.maxForestSize = 10.GiB
        config.mergeInterval = (-1).minutes
        mode.withChronoStore(config) { chronoStore ->
            chronoStore.transaction { tx ->
                val data = tx.createNewStore("data", versioned = true)
                repeat(numberOfEntries) { i ->
                    data.put(createKey(i), "value#${i}")
                }
                tx.commit()
            }

            // start iterating in a new transaction
            val tx1 = chronoStore.beginTransaction()
            val c1 = tx1.getStore("data").openCursorOnLatest()
            c1.firstOrThrow()
            val c1Sequence1 = c1.ascendingEntrySequenceFromHere()

            // flush the in-memory stores to the VFS
            chronoStore.mergeService.flushAllInMemoryStoresToDisk()

            val c1Result = c1Sequence1.toList()
            assertListEntries(c1Result)

            c1.firstOrThrow()

            val tx2 = chronoStore.beginTransaction()
            val c2 = tx2.getStore("data").openCursorOnLatest()

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
    fun canIterateOverAllVersionsWithMultipleFiles(mode: ChronoStoreMode) {
        val config = ChronoStoreConfiguration()
        // disable flushing and merging, we will do it manually in this test
        config.maxForestSize = 10.GiB
        config.mergeInterval = (-1).minutes

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
            val commitTimestamp1 = chronoStore.transaction { tx ->
                val data = tx.createNewStore("data", versioned = true)
                repeat(numberOfEntries) { i ->
                    data.put(createKey(i), "a")
                }
                tx.commit()
            }
            val commitTimestamp2 = chronoStore.transaction { tx ->
                val data = tx.getStore("data")
                for (i in (0..<numberOfEntries step 3)) {
                    data.put(createKey(i), "b")
                }
                tx.commit()
            }

            // flush the in-memory stores to the VFS
            chronoStore.forest.flushAllInMemoryStoresToDisk()

            val commitTimestamp3 = chronoStore.transaction { tx ->
                val data = tx.getStore("data")
                for (i in (0..<numberOfEntries step 5)) {
                    data.delete(createKey(i))
                }
                tx.commit()
            }

            // flush the in-memory stores to the VFS
            chronoStore.forest.flushAllInMemoryStoresToDisk()

            val commitTimestamp4 = chronoStore.transaction { tx ->
                val data = tx.getStore("data")
                for (i in (0..<numberOfEntries step 7)) {
                    data.put(createKey(i), "c")
                }
                tx.commit()
            }

            val storeDir = vfs.directory("data")

            // since we've performed two flushes, we should have two files in the VFS.
            expectThat(storeDir)
                .get { this.list() }
                .filter { it.endsWith(LSMTreeFile.FILE_EXTENSION) }
                .hasSize(2)

            // iterate over the data.
            chronoStore.transaction { tx ->
                tx.getStore("data").openCursorOnLatest().use { cursor ->
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

                tx.getStore("data").openCursorAtTimestamp(0).use { cursor ->
                    expectThat(cursor.first()).isFalse() // should be empty at timestamp 0
                }

                tx.getStore("data").openCursorAtTimestamp(commitTimestamp1 - 1).use { cursor ->
                    expectThat(cursor.first()).isFalse() // should be empty prior to first insertion
                }

                tx.getStore("data").openCursorAtTimestamp(commitTimestamp1).use { cursor ->
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

                tx.getStore("data").openCursorAtTimestamp(commitTimestamp2).use { cursor ->
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

                tx.getStore("data").openCursorAtTimestamp(commitTimestamp3).use { cursor ->
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

                tx.getStore("data").openCursorAtTimestamp(commitTimestamp4).use { cursor ->
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
                val data = tx.createNewStore("data", versioned = true)
                data.put("hello", "world")
                data.put("john", "doe")
                val math = tx.createNewStore("math", versioned = false)
                math.put("pi", "3.1415")
                math.put("e", "2.7182")
                tx.commit()
            }

            chronoStore.transaction { tx1 ->
                chronoStore.transaction { tx2 ->

                    tx1.getStore("data").put("foo", "bar")
                    tx1.getStore("data").delete("john")
                    tx1.getStore("math").put("zero", "0")
                    tx1.getStore("math").delete("pi")

                    tx1.commit()

                    // tx2 should not see the changes performed by tx1
                    expectThat(tx2) {
                        get { this.getStore("data").getEntriesOnLatestAscending().asStrings() }.containsExactly(
                            "hello" to "world",
                            "john" to "doe"
                        )

                        get { this.getStore("math").getEntriesOnLatestAscending().asStrings() }.containsExactly(
                            "e" to "2.7182",
                            "pi" to "3.1415"
                        )
                    }
                }
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
