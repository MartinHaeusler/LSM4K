package org.chronos.chronostore.wal

import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.StoreId

data class CommandAndStoreName(
    val storeId: StoreId,
    val command: Command,
) {

    override fun toString(): String {
        return "${storeId}::${command}"
    }

}