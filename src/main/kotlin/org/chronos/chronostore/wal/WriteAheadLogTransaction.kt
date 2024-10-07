package org.chronos.chronostore.wal

import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.bytes.Bytes

class WriteAheadLogTransaction(
    val transactionId: TransactionId,
    val commitTSN: TSN,
    val storeIdToCommands: Map<StoreId, List<Command>>,
) {

    init {
        require(!hasDeviatingTSNInCommands()) {
            "${countCommandsWithDeviatingTSNs()} commands have a TSN which deviates from the transaction commit TSN (${commitTSN})!"
        }
    }

    private fun countCommandsWithDeviatingTSNs(): Int {
        return this.storeIdToCommands.values.asSequence().flatten().count(::hasDeviatingTSN)
    }

    private fun hasDeviatingTSNInCommands(): Boolean {
        return this.storeIdToCommands.values.any { commands -> commands.any(::hasDeviatingTSN) }
    }

    private fun hasDeviatingTSN(it: Command): Boolean {
        return it.tsn != commitTSN
    }

    override fun toString(): String {
        return "WALTransaction[${transactionId}@${commitTSN}]"
    }
}