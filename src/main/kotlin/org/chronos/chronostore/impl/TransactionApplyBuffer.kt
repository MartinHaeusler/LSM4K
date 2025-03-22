package org.chronos.chronostore.impl

import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.StoreId

class TransactionApplyBuffer(
    private val maxSizeInBytes: Long,
) {

    private var currentSize = 0L

    private val storeIdToCommands = mutableMapOf<StoreId, MutableList<Command>>()

    fun getModifiedStoreIds(): Set<StoreId> {
        return storeIdToCommands.keys
    }

    fun getCommandsForStore(storeId: StoreId): List<Command> {
        return this.storeIdToCommands[storeId] ?: emptyList()
    }

    fun addOperation(storeId: StoreId, command: Command) {
        this.storeIdToCommands.getOrPut(storeId, ::mutableListOf) += command
        currentSize += command.getBinarySize()
    }

    fun isFull(): Boolean {
        return this.currentSize >= this.maxSizeInBytes
    }

    fun isEmpty(): Boolean {
        return this.storeIdToCommands.isEmpty()
    }


    fun clear() {
        this.storeIdToCommands.clear()
        this.currentSize = 0L
    }

}