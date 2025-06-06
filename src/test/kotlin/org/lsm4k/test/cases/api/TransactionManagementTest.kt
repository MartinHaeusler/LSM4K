package org.lsm4k.test.cases.api

import org.junit.jupiter.params.ParameterizedTest
import org.lsm4k.api.TransactionalStore.Companion.withCursor
import org.lsm4k.api.exceptions.TransactionIsReadOnlyException
import org.lsm4k.api.exceptions.TransactionLockAcquisitionException
import org.lsm4k.test.extensions.transaction.LSM4KTransactionTestExtensions.delete
import org.lsm4k.test.extensions.transaction.LSM4KTransactionTestExtensions.put
import org.lsm4k.test.util.CollectionTestUtils.asStrings
import org.lsm4k.test.util.DatabaseEngineTest
import org.lsm4k.test.util.LSM4KMode
import org.lsm4k.test.util.junit.IntegrationTest
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.containsExactlyInAnyOrder
import kotlin.time.Duration.Companion.milliseconds

@IntegrationTest
class TransactionManagementTest {

    @DatabaseEngineTest
    @ParameterizedTest
    fun canCreateReadOnlyTxWhileReadWriteTxIsActive(mode: LSM4KMode) {
        mode.withDatabaseEngine { engine ->
            engine.readWriteTransaction(1.milliseconds) {
                engine.readOnlyTransaction {
                    true
                }
            }
        }
    }

    @DatabaseEngineTest
    @ParameterizedTest
    fun canCreateReadOnlyTxWhileExclusiveTxIsActive(mode: LSM4KMode) {
        mode.withDatabaseEngine { engine ->
            engine.exclusiveTransaction(1.milliseconds) {
                engine.readOnlyTransaction {
                    false
                }
            }
        }
    }

    @DatabaseEngineTest
    @ParameterizedTest
    fun canCreateReadWriteTxWhileReadWriteTxIsActive(mode: LSM4KMode) {
        mode.withDatabaseEngine { engine ->
            engine.readWriteTransaction(1.milliseconds) {
                engine.readWriteTransaction(1.milliseconds) {
                    true
                }
            }
        }
    }

    @DatabaseEngineTest
    @ParameterizedTest
    fun canNotCreateExclusiveTxWhileReadWriteTxIsActive(mode: LSM4KMode) {
        mode.withDatabaseEngine { engine ->
            engine.readWriteTransaction(1.milliseconds) {
                expectThrows<TransactionLockAcquisitionException> {
                    engine.exclusiveTransaction(1.milliseconds) {
                        false
                    }
                }
            }
        }
    }

    @DatabaseEngineTest
    @ParameterizedTest
    fun canNotCreateExclusiveTxWhileExclusiveTxIsActive(mode: LSM4KMode) {
        mode.withDatabaseEngine { engine ->
            engine.exclusiveTransaction(1.milliseconds) {
                expectThrows<TransactionLockAcquisitionException> {
                    engine.exclusiveTransaction(1.milliseconds) {
                        false
                    }
                }
            }
        }
    }

    @DatabaseEngineTest
    @ParameterizedTest
    fun canNotCommitReadOnlyTransaction(mode: LSM4KMode) {
        mode.withDatabaseEngine { engine ->
            expectThrows<TransactionIsReadOnlyException> {
                engine.readOnlyTransaction { tx ->
                    tx.commit()
                }
            }
        }
    }

    @DatabaseEngineTest
    @ParameterizedTest
    fun canNotCreateStoreInReadOnlyTransaction(mode: LSM4KMode) {
        mode.withDatabaseEngine { engine ->
            expectThrows<TransactionIsReadOnlyException> {
                engine.readOnlyTransaction { tx ->
                    tx.createNewStore("test")
                }
            }
        }
    }

    @DatabaseEngineTest
    @ParameterizedTest
    fun canApplyTransientModificationsInReadOnlyTransaction(mode: LSM4KMode) {
        mode.withDatabaseEngine { engine ->
            engine.readWriteTransaction { tx ->
                val test = tx.createNewStore("test")
                test.put("hello", "world")
                test.put("deleteMe", "nothing")
                tx.commit()
            }
            engine.readOnlyTransaction { tx ->
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