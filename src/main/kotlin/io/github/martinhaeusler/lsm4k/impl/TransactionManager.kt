package io.github.martinhaeusler.lsm4k.impl

import com.google.common.collect.MapMaker
import io.github.martinhaeusler.lsm4k.api.LSM4KTransaction
import io.github.martinhaeusler.lsm4k.api.StoreManager
import io.github.martinhaeusler.lsm4k.api.TransactionMode
import io.github.martinhaeusler.lsm4k.api.exceptions.CommitException
import io.github.martinhaeusler.lsm4k.api.exceptions.TransactionIsReadOnlyException
import io.github.martinhaeusler.lsm4k.api.exceptions.TransactionLockAcquisitionException
import io.github.martinhaeusler.lsm4k.impl.transaction.LSM4KTransactionImpl
import io.github.martinhaeusler.lsm4k.util.ManagerState
import io.github.martinhaeusler.lsm4k.util.StoreId
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.util.TransactionId
import io.github.martinhaeusler.lsm4k.util.report.TransactionReport
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsReporter
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.MiB
import io.github.martinhaeusler.lsm4k.wal.WriteAheadLog
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.min
import kotlin.time.Duration

class TransactionManager(
    val storeManager: StoreManager,
    val tsnManager: TSNManager,
    val writeAheadLog: WriteAheadLog,
    val statisticsReporter: StatisticsReporter,
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
    private val openTransactions: ConcurrentMap<TransactionId, LSM4KTransactionImpl> = MapMaker()
        .weakValues()
        .makeMap()

    private val openReadWriteTransactions = AtomicInteger(0)
    private val openExclusiveTransactions = AtomicInteger(0)

    private val transactionCreationLock = ReentrantLock(true)
    private val readWriteTransactionClosed: Condition = transactionCreationLock.newCondition()

    private val commitLock = ReentrantLock(true)

    private val transactionCounter = AtomicLong(0)

    @Volatile
    private var state: ManagerState = ManagerState.OPEN


    // =================================================================================================================
    // TRANSACTION OPENING
    // =================================================================================================================

    fun createNewReadOnlyTransaction(): LSM4KTransaction {
        this.state.checkOpen()
        // NOTE: read-only transactions do not require the transaction creation lock, because they can
        // run in parallel to read-write transactions and exclusive transactions.
        this.transactionCounter.incrementAndGet()
        val newTx = LSM4KTransactionImpl(
            id = TransactionId.randomUUID(),
            lastVisibleSerialNumber = this.tsnManager.getLastReturnedTSN(),
            mode = TransactionMode.READ_ONLY,
            storeManager = this.storeManager,
            transactionManager = this,
            createdAtWallClockTime = System.currentTimeMillis(),
            statisticsReporter = this.statisticsReporter,
        )
        this.statisticsReporter.reportTransactionOpened(TransactionMode.READ_ONLY)
        this.openTransactions[newTx.id] = newTx
        return newTx
    }

    fun createNewReadWriteTransaction(lockAcquisitionTimeout: Duration?): LSM4KTransaction {
        this.state.checkOpen()

        val waitStart = System.currentTimeMillis()
        val maxWaitTimeInMillis = lockAcquisitionTimeout?.inWholeMilliseconds ?: Long.MAX_VALUE
        this.transactionCreationLock.withLock {
            while (this.openExclusiveTransactions.get() > 0) {
                // there's an exclusive transaction going on, we have to wait until it's done.

                // is there any wait time left?
                val now = System.currentTimeMillis()
                val remainingTimeMillis = maxWaitTimeInMillis - (now - waitStart)

                if (remainingTimeMillis <= 0) {
                    throw TransactionLockAcquisitionException(
                        "Could not create a read-write transaction within the configured" +
                            " timeout (${lockAcquisitionTimeout}) due to an ongoing concurrent" +
                            " exclusive transaction!"
                    )
                }
                this.readWriteTransactionClosed.await(remainingTimeMillis, TimeUnit.MILLISECONDS)
            }

            // no concurrent exclusive transactions are going on -> let's create our transaction
            this.transactionCounter.incrementAndGet()
            this.openReadWriteTransactions.incrementAndGet()
            val newTx = LSM4KTransactionImpl(
                id = TransactionId.randomUUID(),
                lastVisibleSerialNumber = this.tsnManager.getLastReturnedTSN(),
                mode = TransactionMode.READ_WRITE,
                storeManager = this.storeManager,
                transactionManager = this,
                createdAtWallClockTime = System.currentTimeMillis(),
                statisticsReporter = this.statisticsReporter,
            )
            this.statisticsReporter.reportTransactionOpened(TransactionMode.READ_WRITE)
            this.openTransactions[newTx.id] = newTx
            return newTx
        }
    }

    fun createNewExclusiveTransaction(lockAcquisitionTimeout: Duration?): LSM4KTransaction {
        val waitStart = System.currentTimeMillis()
        val maxWaitTimeInMillis = lockAcquisitionTimeout?.inWholeMilliseconds ?: Long.MAX_VALUE
        this.transactionCreationLock.withLock {
            while (this.openExclusiveTransactions.get() > 0 || this.openReadWriteTransactions.get() > 0) {
                // there's an exclusive or read-write transaction going on, we have to wait until they're done.

                // is there any wait time left?
                val now = System.currentTimeMillis()
                val remainingTimeMillis = maxWaitTimeInMillis - (now - waitStart)

                if (remainingTimeMillis <= 0) {
                    throw TransactionLockAcquisitionException(
                        "Could not create an exclusive transaction within the configured" +
                            " timeout (${lockAcquisitionTimeout}) due to an ongoing concurrent" +
                            " exclusive or read-write transaction!"
                    )
                }
                this.readWriteTransactionClosed.await(remainingTimeMillis, TimeUnit.MILLISECONDS)
            }

            // no concurrent exclusive transactions are going on -> let's create our transaction
            this.transactionCounter.incrementAndGet()
            this.openExclusiveTransactions.incrementAndGet()
            val newTx = LSM4KTransactionImpl(
                id = TransactionId.randomUUID(),
                lastVisibleSerialNumber = this.tsnManager.getLastReturnedTSN(),
                mode = TransactionMode.EXCLUSIVE,
                storeManager = this.storeManager,
                transactionManager = this,
                createdAtWallClockTime = System.currentTimeMillis(),
                statisticsReporter = this.statisticsReporter,
            )
            this.statisticsReporter.reportTransactionOpened(TransactionMode.EXCLUSIVE)
            this.openTransactions[newTx.id] = newTx
            return newTx
        }
    }

    fun getSmallestOpenReadTSN(): TSN? {
        this.state.checkOpen()
        return this.openTransactions.values.minOfOrNull { it.lastVisibleSerialNumber }
    }

    fun getOpenTransactionIdsAndTSNs(): Map<TransactionId, TSN> {
        this.state.checkOpen()
        return this.openTransactions.values.associate { it.id to it.lastVisibleSerialNumber }
    }


    // =================================================================================================================
    // TRANSACTION CLOSING
    // =================================================================================================================

    fun performCommit(tx: LSM4KTransactionImpl): TSN {
        this.state.checkOpen()

        if (tx.mode == TransactionMode.READ_ONLY) {
            throw TransactionIsReadOnlyException("This transaction is read-only and cannot be committed! Please use rollback() or close() instead.")
        }

        val commitTSN = this.commitLock.withLock {
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
            commitTSN
        }
        this.openTransactions.remove(tx.id)
        tx.close()
        this.statisticsReporter.reportTransactionCommit(tx.mode)
        when (tx.mode) {
            TransactionMode.READ_ONLY -> {
                // no-op
            }

            TransactionMode.READ_WRITE -> this.transactionCreationLock.withLock {
                this.openReadWriteTransactions.decrementAndGet()
                this.readWriteTransactionClosed.signalAll()
            }

            TransactionMode.EXCLUSIVE -> this.transactionCreationLock.withLock {
                this.openExclusiveTransactions.decrementAndGet()
                this.readWriteTransactionClosed.signalAll()
            }
        }
        return commitTSN
    }

    fun performRollback(tx: LSM4KTransactionImpl, isDangling: Boolean) {
        log.trace {
            val qualifier = if (isDangling) {
                "dangling "
            } else {
                ""
            }
            "Rolled back ${qualifier}transaction ${tx.id}."
        }
        if (isDangling) {
            this.statisticsReporter.reportTransactionDangling(tx.mode)
        } else {
            this.statisticsReporter.reportTransactionRollback(tx.mode)
        }
        this.openTransactions.remove(tx.id)
    }

    private fun checkAllModifiedStoresAreWriteable(
        tx: LSM4KTransactionImpl,
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

    private fun applyTransactionToLSMTrees(commitTSN: TSN, tx: LSM4KTransactionImpl) {
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

    // =================================================================================================================
    // STATUS REPORTING
    // =================================================================================================================

    fun report(): TransactionReport {
        val oldestOpenTransactionCreatedAt = this.openTransactions.values.minOfOrNull { it.createdAtWallClockTime }
        val oldestTransactionRuntime = if (oldestOpenTransactionCreatedAt == null) {
            null
        } else {
            min(0L, System.currentTimeMillis() - oldestOpenTransactionCreatedAt)
        }
        return TransactionReport(
            transactionsProcessed = this.transactionCounter.get(),
            openTransactions = this.openTransactions.size,
            longestOpenTransactionDurationInMilliseconds = oldestTransactionRuntime,
        )
    }

    // =================================================================================================================
    // CLOSE HANDLING
    // =================================================================================================================

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