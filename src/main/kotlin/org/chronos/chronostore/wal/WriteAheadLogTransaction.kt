package org.chronos.chronostore.wal

import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.TransactionId

class WriteAheadLogTransaction(
    val transactionId: TransactionId,
    val commitTimestamp: Timestamp,
    val storeIdToCommands: Map<StoreId, List<Command>>,
    val commitMetadata: Bytes,
) {

    init {
        val deviatingCommands = this.storeIdToCommands.values.asSequence().flatten().filter { it.timestamp != commitTimestamp }.toList()
        require(deviatingCommands.isEmpty()){
            "${deviatingCommands.size} commands have a timestamp which deviates from the transaction commit timestamp (${commitTimestamp})!"
        }
    }

    override fun toString(): String {
        return "WALTransaction[${transactionId}@${commitTimestamp}]"
    }
}