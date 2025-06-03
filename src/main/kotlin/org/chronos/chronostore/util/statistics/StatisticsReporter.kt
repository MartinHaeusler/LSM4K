package org.chronos.chronostore.util.statistics

import org.chronos.chronostore.api.TransactionMode

interface StatisticsReporter {

    fun reportCursorOpened(cursorType: String)

    fun reportCursorClosed(cursorType: String)

    fun reportCursorOperationFirst(cursorType: String)

    fun reportCursorOperationLast(cursorType: String)

    fun reportCursorOperationNext(cursorType: String)

    fun reportCursorOperationPrevious(cursorType: String)

    fun reportCursorOperationSeekExactlyOrNext(cursorType: String)

    fun reportCursorOperationSeekExactlyOrPrevious(cursorType: String)

    fun reportBlockCacheRequest()

    fun reportBlockCacheMiss()

    fun reportBlockCacheEviction()

    fun reportBlockLoadTime(millis: Long)

    fun reportFileHeaderCacheRequest()

    fun reportFileHeaderCacheEviction()

    fun reportFileHeaderCacheMiss()

    fun reportStallTime(millis: Long)

    fun reportTransactionOpened(mode: TransactionMode)

    fun reportTransactionCommit(mode: TransactionMode)

    fun reportTransactionRollback(mode: TransactionMode)

    fun reportTransactionDangling(mode: TransactionMode)

    fun reportFlushTaskExecution(executionTimeMillis: Long, writtenBytes: Long, writtenEntries: Long)

    fun reportCompressionInvocation(inputBytes: Long, outputBytes: Long)

    fun reportCompressionInvocation(inputBytes: Int, outputBytes: Int) {
        this.reportCompressionInvocation(inputBytes.toLong(), outputBytes.toLong())
    }

    fun reportDecompressionInvocation(inputBytes: Int, outputBytes: Int) {
        this.reportDecompressionInvocation(inputBytes.toLong(), outputBytes.toLong())
    }

    fun reportDecompressionInvocation(inputBytes: Long, outputBytes: Long)

}