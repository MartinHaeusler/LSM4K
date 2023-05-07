package org.chronos.chronostore.test.cases.api

import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.TransactionBoundStore
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.getLatest
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.put
import org.chronos.chronostore.test.util.ChronoStoreMode
import org.chronos.chronostore.test.util.ChronoStoreTest
import org.chronos.chronostore.util.Bytes
import strikt.api.expectThat
import strikt.assertions.*

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
    fun canCreateStoreAndIterateWithCursor() {
        TODO("implement this test!")
    }

}
