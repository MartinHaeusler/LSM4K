package org.chronos.chronostore.test.cases.api

import org.chronos.chronostore.api.TransactionalStore.Companion.withCursor
import org.chronos.chronostore.api.exceptions.TransactionIsReadOnlyException
import org.chronos.chronostore.api.exceptions.TransactionLockAcquisitionException
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.delete
import org.chronos.chronostore.test.extensions.transaction.ChronoStoreTransactionTestExtensions.put
import org.chronos.chronostore.test.util.ChronoStoreMode
import org.chronos.chronostore.test.util.ChronoStoreTest
import org.chronos.chronostore.test.util.CollectionTestUtils.asStrings
import org.chronos.chronostore.test.util.junit.IntegrationTest
import org.junit.jupiter.params.ParameterizedTest
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.containsExactlyInAnyOrder
import kotlin.time.Duration.Companion.milliseconds

@IntegrationTest
class TransactionManagementTest {

    @ChronoStoreTest
    @ParameterizedTest
    fun canCreateReadOnlyTxWhileReadWriteTxIsActive(mode: ChronoStoreMode) {
        mode.withChronoStore { chronoStore ->
            chronoStore.readWriteTransaction(1.milliseconds) {
                chronoStore.readOnlyTransaction {
                    true
                }
            }
        }
    }

    @ChronoStoreTest
    @ParameterizedTest
    fun canCreateReadOnlyTxWhileExclusiveTxIsActive(mode: ChronoStoreMode) {
        mode.withChronoStore { chronoStore ->
            chronoStore.exclusiveTransaction(1.milliseconds) {
                chronoStore.readOnlyTransaction {
                    false
                }
            }
        }
    }

    @ChronoStoreTest
    @ParameterizedTest
    fun canCreateReadWriteTxWhileReadWriteTxIsActive(mode: ChronoStoreMode) {
        mode.withChronoStore { chronoStore ->
            chronoStore.readWriteTransaction(1.milliseconds) {
                chronoStore.readWriteTransaction(1.milliseconds) {
                    true
                }
            }
        }
    }

    @ChronoStoreTest
    @ParameterizedTest
    fun canNotCreateExclusiveTxWhileReadWriteTxIsActive(mode: ChronoStoreMode) {
        mode.withChronoStore { chronoStore ->
            chronoStore.readWriteTransaction(1.milliseconds) {
                expectThrows<TransactionLockAcquisitionException> {
                    chronoStore.exclusiveTransaction(1.milliseconds) {
                        false
                    }
                }
            }
        }
    }

    @ChronoStoreTest
    @ParameterizedTest
    fun canNotCreateExclusiveTxWhileExclusiveTxIsActive(mode: ChronoStoreMode) {
        mode.withChronoStore { chronoStore ->
            chronoStore.exclusiveTransaction(1.milliseconds) {
                expectThrows<TransactionLockAcquisitionException> {
                    chronoStore.exclusiveTransaction(1.milliseconds) {
                        false
                    }
                }
            }
        }
    }

    @ChronoStoreTest
    @ParameterizedTest
    fun canNotCommitReadOnlyTransaction(mode: ChronoStoreMode) {
        mode.withChronoStore { chronoStore ->
            expectThrows<TransactionIsReadOnlyException> {
                chronoStore.readOnlyTransaction { tx ->
                    tx.commit()
                }
            }
        }
    }

    @ChronoStoreTest
    @ParameterizedTest
    fun canNotCreateStoreInReadOnlyTransaction(mode: ChronoStoreMode) {
        mode.withChronoStore { chronoStore ->
            expectThrows<TransactionIsReadOnlyException> {
                chronoStore.readOnlyTransaction { tx ->
                    tx.createNewStore("test")
                }
            }
        }
    }

    @ChronoStoreTest
    @ParameterizedTest
    fun canApplyTransientModificationsInReadOnlyTransaction(mode: ChronoStoreMode) {
        mode.withChronoStore { chronoStore ->
            chronoStore.readWriteTransaction { tx ->
                val test = tx.createNewStore("test")
                test.put("hello", "world")
                test.put("deleteMe", "nothing")
                tx.commit()
            }
            chronoStore.readOnlyTransaction { tx ->
                val test = tx.getStore("test")

                // this should be ok, as long as we don't commit
                test.put("hello", "test")
                test.put("foo", "bar")
                test.delete("deleteMe")

                val entries = test.withCursor { cursor ->
                    cursor.listAllEntriesAscending().asStrings()
                }

                expectThat(entries).containsExactlyInAnyOrder(
                    Pair("foo", "bar"),
                    Pair("hello", "test"),
                )
            }
        }
    }
}