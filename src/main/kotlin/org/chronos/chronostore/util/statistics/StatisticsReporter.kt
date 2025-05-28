package org.chronos.chronostore.util.statistics

import org.chronos.chronostore.api.TransactionMode
import org.chronos.chronostore.util.cursor.Cursor

interface StatisticsReporter {

    fun reportCursorOpened(cursorType: Class<out Cursor<*, *>>)

    fun reportCursorClosed(cursorType: Class<out Cursor<*, *>>)

    fun reportCursorOperationFirst(cursorType: Class<out Cursor<*, *>>)

    fun reportCursorOperationLast(cursorType: Class<out Cursor<*, *>>)

    fun reportCursorOperationNext(cursorType: Class<out Cursor<*, *>>)

    fun reportCursorOperationPrevious(cursorType: Class<out Cursor<*, *>>)

    fun reportCursorOperationSeekExactlyOrNext(cursorType: Class<out Cursor<*, *>>)

    fun reportCursorOperationSeekExactlyOrPrevious(cursorType: Class<out Cursor<*, *>>)

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