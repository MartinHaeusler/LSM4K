package org.chronos.chronostore.async.tasks

import mu.KotlinLogging
import org.chronos.chronostore.api.exceptions.ChronoStoreFlushException
import org.chronos.chronostore.async.executor.AsyncTaskManager
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.impl.ChronoStoreState
import java.io.InterruptedIOException
import java.nio.channels.ClosedByInterruptException

/**
 * A common interface for all asynchronous tasks.
 *
 * These tasks are executed by the [AsyncTaskManager].
 */
interface AsyncTask {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    /**
     * The human-readable name of this task.
     */
    val name: String

    /**
     * Runs the async task, reporting the progress to the given [monitor].
     *
     * This method needs to be **blocking**, i.e. it must not return until
     * the task has finished completely.
     *
     * Implementations should regularly report progress to the [monitor]
     * as this will also allow for cancellation via [Thread.interrupt].
     *
     * @param monitor The task monitor to report updates to
     */
    fun run(monitor: TaskMonitor)

    /**
     * Handles the exception which has occurred during the task execution.
     *
     * The default behavior is:
     *
     * - If the [chronoStoreState] is [ChronoStoreState.SHUTTING_DOWN] then the [exception]
     *   is very likely caused by an interrupt which was triggered by the shutdown of the
     *   scheduler. Any exception listed in [isExceptionIgnoredDuringShutdown] will be ignored.
     *   All other exceptions will be listed as warnings.
     *
     * - If the [chronoStoreState] is **not** [ChronoStoreState.SHUTTING_DOWN], any exception
     *   will be re-thrown.
     *
     * Individual implementations may override this behavior.
     *
     * @param exception The exception that has been thrown by the [run] method of this task.
     * @param chronoStoreState The current state of the store.
     */
    fun handleTaskException(exception: Exception, chronoStoreState: ChronoStoreState) {
        if (chronoStoreState != ChronoStoreState.SHUTTING_DOWN) {
            log.error(exception) {
                "A fatal exception has occurred during the execution of the asynchronous task '${this.name}'" +
                    " of type '${this.javaClass.name}' on thread '${Thread.currentThread().name}'." +
                    " This may cause deadlocks and/or starvation. The exception was: ${exception}"
            }
            return
        }
        if (isExceptionIgnoredDuringShutdown(exception)) {
            // these exceptions are normal behavior during shutdown and don't need to be logged.
            return
        } else {
            // most likely, these exceptions are not harmful because we're shutting down anyway. Log them.
            log.warn(exception) {
                "An unexpected exception occurred in task '${name}' during ChronoStore shutdown: ${exception}"
            }
        }
    }

    /**
     * Checks if the exception should be ignored during ChronoStore shutdown.
     *
     * @param exception The exception to check
     *
     * @return `true` if the exception should be ignored, otherwise `false`.
     */
    private fun isExceptionIgnoredDuringShutdown(exception: Throwable): Boolean {
        return when (exception) {
            is ChronoStoreFlushException -> {
                val cause = exception.cause
                return if (cause != null && cause !== exception) {
                    isExceptionIgnoredDuringShutdown(cause)
                } else {
                    false
                }
            }

            is InterruptedException -> true
            is InterruptedIOException -> true
            is ClosedByInterruptException -> true
            else -> false
        }
    }

}