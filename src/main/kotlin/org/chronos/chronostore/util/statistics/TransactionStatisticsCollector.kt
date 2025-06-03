package org.chronos.chronostore.util.statistics

import org.chronos.chronostore.api.TransactionMode
import org.chronos.chronostore.api.statistics.TransactionStatisticsReport
import java.util.concurrent.atomic.AtomicLong

/**
 * Collects statistics about transactions with a given [mode].
 */
class TransactionStatisticsCollector(
    val mode: TransactionMode,
) {

    /** How many transactions with this [mode] have been opened? */
    private val openedTransactions = AtomicLong(0)

    /** How many transactions with this [mode] have been committed? */
    private val committedTransactions = AtomicLong(0)

    /** How many transactions with this [mode] have been rolled back? */
    private val rollbackTransactions = AtomicLong(0)

    /** How many transactions with this [mode] have been left dangling (open and unused)? */
    private val danglingTransactions = AtomicLong(0)

    fun reportTransactionOpened() {
        this.openedTransactions.incrementAndGet()
    }

    fun reportTransactionCommit() {
        this.committedTransactions.incrementAndGet()
    }

    fun reportTransactionRollback() {
        this.rollbackTransactions.incrementAndGet()
    }

    fun reportTransactionDangling() {
        this.danglingTransactions.incrementAndGet()
    }

    fun report(): TransactionStatisticsReport {
        return TransactionStatisticsReport(
            mode = this.mode,
            openedTransactions = this.openedTransactions.get(),
            committedTransactions = this.committedTransactions.get(),
            rollbackTransactions = this.rollbackTransactions.get(),
            danglingTransactions = this.danglingTransactions.get(),
        )
    }

    fun reset() {
        this.openedTransactions.set(0)
        this.committedTransactions.set(0)
        this.rollbackTransactions.set(0)
        this.danglingTransactions.set(0)
    }

}