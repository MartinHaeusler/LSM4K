package org.chronos.chronostore.test.cases.api

import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.allEntriesOnLatest
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.delete
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.getLatest
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.put
import org.chronos.chronostore.test.util.ChronoStoreMode
import org.chronos.chronostore.test.util.ChronoStoreTest
import org.chronos.chronostore.util.Bytes
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.fail
import strikt.api.expectThat
import strikt.assertions.*
import kotlin.math.min

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
                val test = tx.store("test")
                test.put("foo", "bar")

                val math = tx.store("math")
                math.put("pi", "3.1415")
                math.put("e", "2.718")

                tx.commit()
            }
            chronoStore.transaction { tx ->
                expectThat(tx) {
                    get { allStores }.hasSize(2).and {
                        any {
                            get { this.store.name }.isEqualTo("test")
                            get { this.getLatest("foo") }.isEqualTo(Bytes("bar"))
                            get { this.getLatest("bullshit") }.isNull()
                        }
                        any {
                            get { this.store.name }.isEqualTo("math")
                            get { this.getLatest("pi") }.isEqualTo(Bytes("3.1415"))
                            get { this.getLatest("e") }.isEqualTo(Bytes("2.718"))
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
                        get { this.store.name }.isEqualTo("test")
                        get { this.getLatest("foo") }.isEqualTo(Bytes("bar"))
                        get { this.getLatest("bullshit") }.isNull()
                    }
                    any {
                        get { this.store.name }.isEqualTo("math")
                        get { this.getLatest("pi") }.isEqualTo(Bytes("3.1415"))
                        get { this.getLatest("e") }.isEqualTo(Bytes("2.718"))
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
                        get { this.store.name }.isEqualTo("test")
                        get { this.allEntriesOnLatest }.containsExactly(Bytes("foo") to Bytes("bar"))
                    }
                    any {
                        get { this.store.name }.isEqualTo("math")
                        get { this.allEntriesOnLatest }.containsExactly(
                            Bytes("e") to Bytes("2.718"),
                            Bytes("pi") to Bytes("3.1415")
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
            if(list.size != numberOfEntries){
                fail("Expected result list to have size ${numberOfEntries}, but found ${list.size}! The existing entries have the expected structure and ordering. The last key is: ${list.lastOrNull()?.first?.asString() ?: "<null>"}")
            }
        }

        val config = ChronoStoreConfiguration()
        // disable flushing and merging, we will do it manually in this test
        config.maxInMemoryTreeSizeInBytes = Long.MAX_VALUE
        config.mergeIntervalMillis = Long.MAX_VALUE
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
            val c1 = tx1.store("data").openCursorOnLatest()
            c1.firstOrThrow()
            val c1Sequence1 = c1.ascendingEntrySequenceFromHere()

            // flush the in-memory stores to the VFS
            chronoStore.mergeService.flushAllInMemoryStoresToDisk()

            val c1Result = c1Sequence1.toList()
            assertListEntries(c1Result)

            c1.firstOrThrow()

            val tx2 = chronoStore.beginTransaction()
            val c2 = tx2.store("data").openCursorOnLatest()

            c2.firstOrThrow()
            val c2Sequence1 = c2.ascendingEntrySequenceFromHere()
            val c1Sequence2 = c1.ascendingEntrySequenceFromHere()

            chronoStore.mergeService.mergeNow(major = true)

            assertListEntries(c1Sequence2.toList())

            assertListEntries(c2Sequence1.toList())

            c1.close()
            tx1.close()
            c2.close()
            tx2.close()
        }
    }

    @ChronoStoreTest
    fun canIterateOverAllVersionsWithMultipleFiles(mode: ChronoStoreMode){
        val config = ChronoStoreConfiguration()
        // disable flushing and merging, we will do it manually in this test
        config.maxInMemoryTreeSizeInBytes = Long.MAX_VALUE
        config.mergeIntervalMillis = Long.MAX_VALUE

        val numberOfEntries = 1000
        mode.withChronoStore(config) { chronoStore ->
            chronoStore.transaction { tx ->
                val data = tx.createNewStore("data", versioned = true)
                repeat(numberOfEntries) { i ->
                    data.put(createKey(i), "a")
                }
                tx.commit()
            }
            chronoStore.transaction { tx ->
                val data = tx.store("data")
                for(i in (0 until numberOfEntries step 3)) {
                    data.put(createKey(i), "b")
                }
                tx.commit()
            }

            // flush the in-memory stores to the VFS
            chronoStore.mergeService.flushAllInMemoryStoresToDisk()

            chronoStore.transaction { tx ->
                val data = tx.store("data")
                for(i in (0 until numberOfEntries step 5)) {
                    data.delete(createKey(i))
                }
                tx.commit()
            }

            // flush the in-memory stores to the VFS
            chronoStore.mergeService.flushAllInMemoryStoresToDisk()

            chronoStore.transaction { tx ->
                val data = tx.store("data")
                for(i in (0 until numberOfEntries step 7)) {
                    data.put(createKey(i), "c")
                }
                tx.commit()
            }

        }
    }

    @ChronoStoreTest
    fun canIsolateTransactionsFromOneAnother(mode: ChronoStoreMode){

    }

    private fun createKey(key: Int): String {
        return "key#${key.toString().padStart(6, '0')}"
    }
}
