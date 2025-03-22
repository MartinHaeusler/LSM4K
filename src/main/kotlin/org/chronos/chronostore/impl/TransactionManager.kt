package org.chronos.chronostore.impl

import com.google.common.collect.MapMaker
import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.StoreManager
import org.chronos.chronostore.api.exceptions.CommitException
import org.chronos.chronostore.impl.transaction.ChronoStoreTransactionImpl
import org.chronos.chronostore.util.ManagerState
import org.chronos.chronostore.util.StoreId
import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.TransactionId
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import org.chronos.chronostore.util.unit.BinarySize.Companion.MiB
import org.chronos.chronostore.wal.WriteAheadLog
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class TransactionManager(
    val storeManager: StoreManager,
    val tsnManager: TSNManager,
    val writeAheadLog: WriteAheadLog,
) : AutoCloseable {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    // we use a map with weak values here because:
    // - If user code opens a transaction...
    // - ... but forgets to close it...
    // - ... and eventually no longer holds a reference to it...
    // - ... then it will disappear from the map and be "cleaned up" automatically.
    // This is important because some cleanup algorithms will look for the open transaction with
    // the smallest "lastVisibleSerialNumber". A dangling transaction may artificially keep this
    // value low for no reason.
    // TODO [FEATURE]: Allow to configure a maximum number of concurrent transactions.
    private val openTransactions: ConcurrentMap<TransactionId, ChronoStoreTransaction> = MapMaker()
        .weakValues()
        .makeMap()

    private val commitLock = ReentrantLock(true)

    @Volatile
    private var state: ManagerState = ManagerState.OPEN

    fun createNewTransaction(): ChronoStoreTransaction {
        this.state.checkOpen()
        ChronoStoreStatistics.TRANSACTIONS.incrementAndGet()
        val newTx = ChronoStoreTransactionImpl(
            id = TransactionId.randomUUID(),
            lastVisibleSerialNumber = this.tsnManager.getLastReturnedTSN(),
            storeManager = this.storeManager,
            transactionManager = this,
        )
        this.openTransactions[newTx.id] = newTx
        return newTx
    }

    fun getSmallestOpenReadTSN(): TSN? {
        this.state.checkOpen()
        return this.openTransactions.values.minOfOrNull { it.lastVisibleSerialNumber }
    }

    fun getOpenTransactionIdsAndTSNs(): Map<TransactionId, TSN> {
        this.state.checkOpen()
        return this.openTransactions.values.associate { it.id to it.lastVisibleSerialNumber }
    }

    fun performCommit(tx: ChronoStoreTransactionImpl): TSN {
        this.state.checkOpen()
        this.commitLock.withLock {
            val commitTSN = this.tsnManager.getUniqueTSN()
            this.storeManager.withStoreReadLock {
                // ensure first that all stores are indeed writeable, otherwise we may
                // write something into our WAL file which isn't actually "legal".
                this.checkAllModifiedStoresAreWriteable(tx)

                // once we know that all target stores exist, add it to the WAL
                this.writeAheadLog.addCommittedTransaction(commitTSN, tx.getChangesAsSequence(commitTSN))

                // WAL operation has completed successfully.
                // Forward the changes to the LSM trees
                this.applyTransactionToLSMTrees(commitTSN, tx)
            }
            log.trace { "Performed commit of transaction ${tx.id}. Transaction TSN: ${tx.lastVisibleSerialNumber}, commit TSN: ${commitTSN}." }
            this.openTransactions.remove(tx.id)
            tx.close()
            ChronoStoreStatistics.TRANSACTION_COMMITS.incrementAndGet()
            return commitTSN
        }
    }

    private fun checkAllModifiedStoresAreWriteable(
        tx: ChronoStoreTransactionImpl,
    ) {
        for (storeId in tx.modifiedStoreIds) {
            val store = this.storeManager.getStoreByIdOrNull(tx, storeId)
                ?: throw CommitException(
                    "Transaction could not be fully committed, because the" +
                        " store '${storeId}' does not exist! Commit will be aborted!"
                )
            if (store.isTerminated) {
                throw CommitException(
                    "Transaction could not be fully committed, because the" +
                        " store '${storeId}' has been terminated and does not accept" +
                        " any further changes! Commit will be aborted!"
                )
            }
        }
    }

    private fun applyTransactionToLSMTrees(commitTSN: TSN, tx: ChronoStoreTransactionImpl) {
        // TODO [FEATURE]: Make transaction apply buffer size configurable
        val writeBuffer = TransactionApplyBuffer(maxSizeInBytes = 64.MiB.bytes)
        val allModifiedStoreIds = mutableSetOf<StoreId>()
        for ((storeId, command) in tx.getChangesAsSequence(commitTSN)) {
            writeBuffer.addOperation(storeId, command)
            if (writeBuffer.isFull()) {
                allModifiedStoreIds += writeBuffer.getModifiedStoreIds()
                flushWriteBuffer(writeBuffer)
            }
        }
        if (!writeBuffer.isEmpty()) {
            allModifiedStoreIds += writeBuffer.getModifiedStoreIds()
            flushWriteBuffer(writeBuffer)
        }
        this.applyLowWatermarkToStores(allModifiedStoreIds, commitTSN)
    }

    private fun flushWriteBuffer(writeBuffer: TransactionApplyBuffer) {
        val storeManagerAdmin = this.storeManager as StoreManagerImpl
        for (storeId in writeBuffer.getModifiedStoreIds()) {
            val store = storeManagerAdmin.getStoreByIdAdmin(storeId)
            val commands = writeBuffer.getCommandsForStore(storeId)
            if (commands.isEmpty()) {
                continue
            }
            store.tree.putAll(commands)
        }
        writeBuffer.clear()
    }

    private fun applyLowWatermarkToStores(storeIds: Set<StoreId>, tsn: TSN) {
        val storeManagerAdmin = this.storeManager as StoreManagerImpl
        for (storeId in storeIds) {
            val store = storeManagerAdmin.getStoreByIdAdmin(storeId)
            store.tree.setHighestCompletelyWrittenTSN(tsn)
        }
    }

    override fun close() {
        this.closeInternal(ManagerState.CLOSED)
    }

    fun closePanic() {
        this.closeInternal(ManagerState.PANIC)
    }

    private fun closeInternal(closeState: ManagerState) {
        if (this.state.isClosed()) {
            this.state = closeState
            return
        }
        this.state = closeState
        for (transaction in this.openTransactions.values) {
            transaction.rollback()
        }
    }

}