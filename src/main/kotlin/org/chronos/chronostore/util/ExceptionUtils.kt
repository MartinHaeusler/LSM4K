package org.chronos.chronostore.util

import org.chronos.chronostore.api.exceptions.FlushException
import java.io.InterruptedIOException
import java.nio.channels.ClosedByInterruptException
import java.util.*

object ExceptionUtils {

    /**
     * The exception classes which can be safely ignored during store shutdown.
     *
     * Subclasses will be ignored as well.
     */
    private val EXCEPTION_CLASSES_TO_IGNORE_DURING_SHUTDOWN = setOf(
        FlushException::class,
        InterruptedException::class,
        InterruptedIOException::class,
        ClosedByInterruptException::class,
    )


    /**
     * Checks if the exception should be ignored during ChronoStore shutdown.
     *
     * @param throwable The exception to check
     *
     * @return `true` if the exception should be ignored, otherwise `false`.
     */
    fun isIgnoredDuringShutdown(throwable: Throwable): Boolean {
        // some exceptions reference themselves as cause, which is weird, but it happens.
        // to avoid endless loops here, we remember where we already have been.
        val visited = mutableSetOf<Throwable>()
        val toVisit = Stack<Throwable>()
        toVisit += throwable
        // walk through the exception "caused by" hierarchy and check if any of them
        // should be ignored.
        while (toVisit.isNotEmpty()) {
            val exception = toVisit.pop()
            if (exception in visited) {
                continue
            }
            visited += exception
            for(ignoredExceptionType in EXCEPTION_CLASSES_TO_IGNORE_DURING_SHUTDOWN){
                if(ignoredExceptionType.isInstance(exception)){
                    return true
                }
            }
            val cause = exception.cause
                ?: continue

            toVisit += cause
        }
        return false
    }

}