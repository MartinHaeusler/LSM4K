package io.github.martinhaeusler.lsm4k.api.statistics

import io.github.martinhaeusler.lsm4k.api.TransactionMode

/**
 * An immutable snapshot report about transaction statistics of a given transaction [mode].
 */
data class TransactionStatisticsReport(
    /** The mode of the transactions this report is about. */
    val mode: TransactionMode,

    /** How many transactions with this [mode] have been opened? */
    val openedTransactions: Long,

    /** How many transactions with this [mode] have been committed? */
    val committedTransactions: Long,

    /** How many transactions with this [mode] have been rolled back? */
    val rollbackTransactions: Long,

    /** How many transactions with this [mode] have been left dangling (open and unused)? */
    val danglingTransactions: Long,
) {

    companion object {

        fun empty(mode: TransactionMode): TransactionStatisticsReport {
            return TransactionStatisticsReport(
                mode = mode,
                openedTransactions = 0L,
                committedTransactions = 0L,
                rollbackTransactions = 0L,
                danglingTransactions = 0L,
            )
        }

        fun Collection<TransactionStatisticsReport>.sum(): TransactionStatisticsReport {
            return this.fold(empty(TransactionMode.READ_WRITE), TransactionStatisticsReport::plus)
        }

    }

    operator fun plus(other: TransactionStatisticsReport): TransactionStatisticsReport {
        return TransactionStatisticsReport(
            // for the lack of a better alternative, we use READ_WRITE in the combined report
            mode = TransactionMode.READ_WRITE,
            openedTransactions = this.openedTransactions + other.openedTransactions,
            committedTransactions = this.committedTransactions + other.committedTransactions,
            rollbackTransactions = this.rollbackTransactions + other.rollbackTransactions,
            danglingTransactions = this.danglingTransactions + other.danglingTransactions,
        )
    }

}