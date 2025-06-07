package io.github.martinhaeusler.lsm4k.util.statistics

import io.github.martinhaeusler.lsm4k.api.DatabaseEngine
import io.github.martinhaeusler.lsm4k.api.TransactionMode
import io.github.martinhaeusler.lsm4k.api.statistics.StatisticsManager
import io.github.martinhaeusler.lsm4k.api.statistics.StatisticsReport
import io.github.martinhaeusler.lsm4k.util.Timestamp
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Collects and [reports][report] on statistics about a [DatabaseEngine] instance.
 */
class StatisticsCollector : StatisticsReporter, StatisticsManager {

    companion object {

        fun active(): StatisticsCollector {
            val collector = StatisticsCollector()
            collector.startCollection()
            return collector
        }

        fun inactive(): StatisticsCollector {
            return StatisticsCollector()
        }

    }

    // =================================================================================================================
    // STATE
    // =================================================================================================================

    /** The most recent final report before stopping the collection of statistics. */
    private var mostRecentReport: StatisticsReport? = null

    /** When did the tracking of events start? */
    @Volatile
    private var trackingStart: Timestamp? = null

    /** Cursor statistics by cursor class name. */
    private val cursorStatisticsCollectors = ConcurrentHashMap<String, CursorStatisticsCollector>()

    /** Transaction statistics by [TransactionMode]. */
    private val transactionStatisticCollectors = TransactionMode.entries.associateWith { TransactionStatisticsCollector(it) }

    private val blockCacheStatisticsCollector = CacheStatisticsCollector()

    private val fileHeaderCacheStatisticsCollector = CacheStatisticsCollector()

    /** How many times did we load a block from raw bytes? */
    private val blockLoads = AtomicLong(0L)

    /** How much time (in milliseconds) did we spend loading blocks? */
    private val blockLoadTime = AtomicLong(0L)

    /** How long have writer threads been stalled because of full in-memory trees? */
    private val totalWriteStallTimeMillis = AtomicLong(0L)

    /** How many times have writer threads been stalled because of full in-memory trees? */
    private val writeStallEvents = AtomicLong(0L)

    /** How many flush tasks have been executed? */
    private val flushTaskExecutions = AtomicLong(0L)

    /** How many bytes have been written by the flush tasks in total? */
    private val flushTaskWrittenBytes = AtomicLong(0L)

    /** How many key-value entries have been written by the flush tasks in total? */
    private val flushTaskWrittenEntries = AtomicLong(0L)

    /** How much time has been spent in total in flush tasks (possibly concurrently)? */
    private val flushTaskTotalTime = AtomicLong(0L)

    /** How many times did we compress raw bytes? */
    private val compressionInvocations = AtomicLong(0L)

    /** How many input bytes did we receive in total for compression? */
    private val compressionInputBytes = AtomicLong(0L)

    /** How many output bytes did we deliver in total for compression? */
    private val compressionOutputBytes = AtomicLong(0L)

    /** How many times did we uncompress bytes? */
    private val decompressionInvocations = AtomicLong(0L)

    /** How many input bytes did we receive in total for decompression? */
    private val decompressionInputBytes = AtomicLong(0L)

    /** How many output bytes did we deliver in total for decompression? */
    private val decompressionOutputBytes = AtomicLong(0L)

    // =================================================================================================================
    // MANAGEMENT AND REPORTING
    // The public user-facing API which allows the user to start and stop the collection as well as generate a report.
    // =================================================================================================================

    override val isCollectionActive: Boolean
        get() = this.trackingStart != null

    override fun report(): StatisticsReport? {
        return this.generateReportForCurrentState()
            ?: this.mostRecentReport
    }

    override fun startCollection(): Boolean {
        if (this.isCollectionActive) {
            return false
        }
        this.restartCollection()
        return true
    }

    override fun stopCollection(): Boolean {
        if (!this.isCollectionActive) {
            return false
        }
        this.mostRecentReport = this.generateReportForCurrentState()
        this.reset()
        return true
    }

    override fun restartCollection() {
        this.reset()
        this.trackingStart = System.currentTimeMillis()
    }

    private fun reset() {
        this.trackingStart = null
        this.cursorStatisticsCollectors.clear()
        this.transactionStatisticCollectors.values.forEach { it.reset() }
        this.blockCacheStatisticsCollector.reset()
        this.fileHeaderCacheStatisticsCollector.reset()
        this.blockLoads.set(0)
        this.blockLoadTime.set(0)
        this.totalWriteStallTimeMillis.set(0)
        this.writeStallEvents.set(0)
        this.flushTaskExecutions.set(0)
        this.flushTaskWrittenBytes.set(0)
        this.flushTaskWrittenEntries.set(0)
        this.flushTaskTotalTime.set(0)
        this.compressionInvocations.set(0)
        this.compressionInputBytes.set(0)
        this.compressionOutputBytes.set(0)
        this.decompressionInvocations.set(0)
        this.decompressionInputBytes.set(0)
        this.decompressionOutputBytes.set(0)
    }

    private fun generateReportForCurrentState(): StatisticsReport? {
        val startTimestamp = this.trackingStart
            ?: return null
        return StatisticsReport(
            trackingStartTimestamp = startTimestamp,
            trackingEndTimestamp = System.currentTimeMillis(),
            cursorStatistics = this.cursorStatisticsCollectors.mapValues { it.value.report() },
            transactionStatistics = this.transactionStatisticCollectors.mapValues { it.value.report() },
            blockCacheStatistics = this.blockCacheStatisticsCollector.report(),
            fileHeaderCacheStatistics = this.fileHeaderCacheStatisticsCollector.report(),
            blockLoads = this.blockLoads.get(),
            blockLoadTime = this.blockLoadTime.get(),
            totalWriteStallTimeMillis = this.totalWriteStallTimeMillis.get(),
            writeStallEvents = this.writeStallEvents.get(),
            flushTaskExecutions = this.flushTaskExecutions.get(),
            flushTaskWrittenBytes = this.flushTaskWrittenBytes.get(),
            flushTaskWrittenEntries = this.flushTaskWrittenEntries.get(),
            flushTaskTotalTime = this.flushTaskTotalTime.get(),
            compressionInvocations = this.compressionInvocations.get(),
            compressionInputBytes = this.compressionInputBytes.get(),
            compressionOutputBytes = this.compressionOutputBytes.get(),
            decompressionInvocations = this.decompressionInvocations.get(),
            decompressionInputBytes = this.decompressionInputBytes.get(),
            decompressionOutputBytes = this.decompressionOutputBytes.get(),
        )
    }

    // =================================================================================================================
    // DATA COLLECTION
    // These methods are used by the various components to report upon their actions.
    // =================================================================================================================

    override fun reportCursorOpened(cursorType: String) {
        if (!this.isCollectionActive) {
            return
        }
        this.getCursorStats(cursorType).reportCursorOpened()
    }

    override fun reportCursorClosed(cursorType: String) {
        if (!this.isCollectionActive) {
            return
        }
        this.getCursorStats(cursorType).reportCursorClosed()
    }

    override fun reportCursorOperationFirst(cursorType: String) {
        if (!this.isCollectionActive) {
            return
        }
        this.getCursorStats(cursorType).reportCursorOperationFirst()
    }

    override fun reportCursorOperationLast(cursorType: String) {
        if (!this.isCollectionActive) {
            return
        }
        this.getCursorStats(cursorType).reportCursorOperationLast()
    }

    override fun reportCursorOperationNext(cursorType: String) {
        if (!this.isCollectionActive) {
            return
        }
        this.getCursorStats(cursorType).reportCursorOperationNext()
    }

    override fun reportCursorOperationPrevious(cursorType: String) {
        if (!this.isCollectionActive) {
            return
        }
        this.getCursorStats(cursorType).reportCursorOperationPrevious()
    }

    override fun reportCursorOperationSeekExactlyOrNext(cursorType: String) {
        if (!this.isCollectionActive) {
            return
        }
        this.getCursorStats(cursorType).reportCursorOperationSeekExactlyOrNext()
    }

    override fun reportCursorOperationSeekExactlyOrPrevious(cursorType: String) {
        if (!this.isCollectionActive) {
            return
        }
        this.getCursorStats(cursorType).reportCursorOperationSeekExactlyOrPrevious()
    }

    override fun reportBlockCacheRequest() {
        if (!this.isCollectionActive) {
            return
        }
        this.blockCacheStatisticsCollector.reportCacheRequest()
    }

    override fun reportBlockCacheMiss() {
        if (!this.isCollectionActive) {
            return
        }
        this.blockCacheStatisticsCollector.reportCacheMiss()
    }

    override fun reportBlockCacheEviction() {
        if (!this.isCollectionActive) {
            return
        }
        this.blockCacheStatisticsCollector.reportCacheEviction()
    }

    override fun reportBlockLoadTime(millis: Long) {
        if (!this.isCollectionActive) {
            return
        }
        this.blockLoads.incrementAndGet()
        this.blockLoadTime.getAndAdd(millis)
    }

    override fun reportFileHeaderCacheRequest() {
        if (!this.isCollectionActive) {
            return
        }
        this.fileHeaderCacheStatisticsCollector.reportCacheRequest()
    }

    override fun reportFileHeaderCacheEviction() {
        if (!this.isCollectionActive) {
            return
        }
        this.fileHeaderCacheStatisticsCollector.reportCacheEviction()
    }

    override fun reportFileHeaderCacheMiss() {
        if (!this.isCollectionActive) {
            return
        }
        this.fileHeaderCacheStatisticsCollector.reportCacheMiss()
    }

    override fun reportStallTime(millis: Long) {
        if (!this.isCollectionActive) {
            return
        }
        this.totalWriteStallTimeMillis.addAndGet(millis)
        this.writeStallEvents.incrementAndGet()
    }

    override fun reportTransactionOpened(mode: TransactionMode) {
        if (!this.isCollectionActive) {
            return
        }
        this.transactionStatisticCollectors.getValue(mode).reportTransactionOpened()
    }

    override fun reportTransactionCommit(mode: TransactionMode) {
        if (!this.isCollectionActive) {
            return
        }
        this.transactionStatisticCollectors.getValue(mode).reportTransactionCommit()
    }

    override fun reportTransactionRollback(mode: TransactionMode) {
        if (!this.isCollectionActive) {
            return
        }
        this.transactionStatisticCollectors.getValue(mode).reportTransactionRollback()
    }

    override fun reportTransactionDangling(mode: TransactionMode) {
        if (!this.isCollectionActive) {
            return
        }
        this.transactionStatisticCollectors.getValue(mode).reportTransactionDangling()
    }

    override fun reportFlushTaskExecution(executionTimeMillis: Long, writtenBytes: Long, writtenEntries: Long) {
        if (!this.isCollectionActive) {
            return
        }
        this.flushTaskTotalTime.getAndAdd(executionTimeMillis)
        this.flushTaskWrittenEntries.getAndAdd(writtenEntries)
        this.flushTaskWrittenBytes.getAndAdd(writtenBytes)
        this.flushTaskExecutions.incrementAndGet()
    }

    override fun reportCompressionInvocation(inputBytes: Long, outputBytes: Long) {
        if (!this.isCollectionActive) {
            return
        }
        this.compressionInvocations.incrementAndGet()
        this.compressionInputBytes.getAndAdd(inputBytes)
        this.compressionOutputBytes.getAndAdd(outputBytes)
    }

    override fun reportDecompressionInvocation(inputBytes: Long, outputBytes: Long) {
        if (!this.isCollectionActive) {
            return
        }
        this.decompressionInvocations.incrementAndGet()
        this.decompressionInputBytes.getAndAdd(inputBytes)
        this.decompressionOutputBytes.getAndAdd(outputBytes)
    }

    private fun getCursorStats(cursorType: String): CursorStatisticsCollector {
        return this.cursorStatisticsCollectors.computeIfAbsent(cursorType) {
            CursorStatisticsCollector(it)
        }
    }


}