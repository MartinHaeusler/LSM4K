package org.chronos.chronostore.util.statistics

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.TransactionMode
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.statistics.report.StatisticsReport
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Collects and [reports][report] on statistics about a [ChronoStore] instance.
 */
class StatisticsCollector : StatisticsReporter {

    /** When did the tracking of events start? */
    private var trackingStart = System.currentTimeMillis()

    /** Cursor statistics by cursor class. */
    private val cursorStatisticsCollectors = ConcurrentHashMap<Class<out Cursor<*, *>>, CursorStatisticsCollector>()

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

    override fun reportCursorOpened(cursorType: Class<out Cursor<*, *>>) {
        this.getCursorStats(cursorType).reportCursorOpened()
    }

    override fun reportCursorClosed(cursorType: Class<out Cursor<*, *>>) {
        this.getCursorStats(cursorType).reportCursorClosed()
    }

    override fun reportCursorOperationFirst(cursorType: Class<out Cursor<*, *>>) {
        this.getCursorStats(cursorType).reportCursorOperationFirst()
    }

    override fun reportCursorOperationLast(cursorType: Class<out Cursor<*, *>>) {
        this.getCursorStats(cursorType).reportCursorOperationLast()
    }

    override fun reportCursorOperationNext(cursorType: Class<out Cursor<*, *>>) {
        this.getCursorStats(cursorType).reportCursorOperationNext()
    }

    override fun reportCursorOperationPrevious(cursorType: Class<out Cursor<*, *>>) {
        this.getCursorStats(cursorType).reportCursorOperationPrevious()
    }

    override fun reportCursorOperationSeekExactlyOrNext(cursorType: Class<out Cursor<*, *>>) {
        this.getCursorStats(cursorType).reportCursorOperationSeekExactlyOrNext()
    }

    override fun reportCursorOperationSeekExactlyOrPrevious(cursorType: Class<out Cursor<*, *>>) {
        this.getCursorStats(cursorType).reportCursorOperationSeekExactlyOrPrevious()
    }

    override fun reportBlockCacheRequest() {
        this.blockCacheStatisticsCollector.reportCacheRequest()
    }

    override fun reportBlockCacheMiss() {
        this.blockCacheStatisticsCollector.reportCacheMiss()
    }

    override fun reportBlockCacheEviction() {
        this.blockCacheStatisticsCollector.reportCacheEviction()
    }

    override fun reportBlockLoadTime(millis: Long) {
        this.blockLoads.incrementAndGet()
        this.blockLoadTime.getAndAdd(millis)
    }

    override fun reportFileHeaderCacheRequest() {
        this.fileHeaderCacheStatisticsCollector.reportCacheRequest()
    }

    override fun reportFileHeaderCacheEviction() {
        this.fileHeaderCacheStatisticsCollector.reportCacheEviction()
    }

    override fun reportFileHeaderCacheMiss() {
        this.fileHeaderCacheStatisticsCollector.reportCacheMiss()
    }

    override fun reportStallTime(millis: Long) {
        this.totalWriteStallTimeMillis.addAndGet(millis)
        this.writeStallEvents.incrementAndGet()
    }

    override fun reportTransactionOpened(mode: TransactionMode) {
        this.transactionStatisticCollectors.getValue(mode).reportTransactionOpened()
    }

    override fun reportTransactionCommit(mode: TransactionMode) {
        this.transactionStatisticCollectors.getValue(mode).reportTransactionCommit()
    }

    override fun reportTransactionRollback(mode: TransactionMode) {
        this.transactionStatisticCollectors.getValue(mode).reportTransactionRollback()
    }

    override fun reportTransactionDangling(mode: TransactionMode) {
        this.transactionStatisticCollectors.getValue(mode).reportTransactionDangling()
    }

    override fun reportFlushTaskExecution(executionTimeMillis: Long, writtenBytes: Long, writtenEntries: Long) {
        this.flushTaskTotalTime.getAndAdd(executionTimeMillis)
        this.flushTaskWrittenEntries.getAndAdd(writtenEntries)
        this.flushTaskWrittenBytes.getAndAdd(writtenBytes)
        this.flushTaskExecutions.incrementAndGet()

    }

    override fun reportCompressionInvocation(inputBytes: Long, outputBytes: Long) {
        this.compressionInvocations.incrementAndGet()
        this.compressionInputBytes.getAndAdd(inputBytes)
        this.compressionOutputBytes.getAndAdd(outputBytes)
    }

    override fun reportDecompressionInvocation(inputBytes: Long, outputBytes: Long) {
        this.decompressionInvocations.incrementAndGet()
        this.decompressionInputBytes.getAndAdd(inputBytes)
        this.decompressionOutputBytes.getAndAdd(outputBytes)
    }

    private fun getCursorStats(cursorType: Class<out Cursor<*, *>>): CursorStatisticsCollector {
        return this.cursorStatisticsCollectors.computeIfAbsent(cursorType) {
            CursorStatisticsCollector(it)
        }
    }

    /**
     * Generates an immutable snapshot report about the current statistics.
     */
    fun report(): StatisticsReport {
        return StatisticsReport(
            trackingStartTimestamp = this.trackingStart,
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

    fun reset() {
        this.trackingStart = System.currentTimeMillis()
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
}