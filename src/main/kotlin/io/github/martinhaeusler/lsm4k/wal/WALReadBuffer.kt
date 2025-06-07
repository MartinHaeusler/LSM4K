package io.github.martinhaeusler.lsm4k.wal

import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.TSN
import kotlin.math.max

class WALReadBuffer(
    private val maxSizeInBytes: Long,
    private val storeIdToLowWatermark: Map<StoreId, TSN?>,
) {

    private var currentSize = 0L

    private val storeIdToCompletedTSN = mutableMapOf<StoreId, TSN>()
    private val storeIdToCommands = mutableMapOf<StoreId, MutableList<Command>>()


    fun getModifiedStoreIds(): Set<StoreId> {
        return this.storeIdToCommands.keys + this.storeIdToCompletedTSN.keys
    }

    fun getCommandsForStore(storeId: StoreId): List<Command> {
        return this.storeIdToCommands[storeId] ?: emptyList()
    }

    fun getCompletedTSNForStore(storeId: StoreId): TSN? {
        return this.storeIdToCompletedTSN[storeId]
    }

    fun addOperation(storeId: StoreId, command: Command) {
        if (!needsReplay(storeId, command.tsn)) {
            // this store has this command persisted already, skip.
            return
        }

        this.storeIdToCommands.getOrPut(storeId, ::mutableListOf) += command
        currentSize += command.getBinarySize()
    }

    fun completeTransaction(modifiedStoreIds: Set<StoreId>, commitTSN: TSN) {
        for (storeId in modifiedStoreIds) {
            val existing = this.storeIdToCompletedTSN[storeId] ?: -1
            this.storeIdToCompletedTSN[storeId] = max(commitTSN, existing)
        }
    }

    fun isFull(): Boolean {
        return this.currentSize >= this.maxSizeInBytes
    }

    fun isEmpty(): Boolean {
        return this.storeIdToCompletedTSN.isEmpty() && this.storeIdToCommands.isEmpty()
    }


    fun clear() {
        this.storeIdToCommands.clear()
        this.storeIdToCompletedTSN.clear()
        this.currentSize = 0L
    }

    private fun needsReplay(storeId: StoreId, commitTSN: TSN): Boolean {
        val lowWatermark = this.storeIdToLowWatermark[storeId] ?: -1
        return commitTSN > lowWatermark
    }

}