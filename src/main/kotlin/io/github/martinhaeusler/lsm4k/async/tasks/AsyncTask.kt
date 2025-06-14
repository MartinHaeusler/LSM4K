package io.github.martinhaeusler.lsm4k.async.tasks

import io.github.martinhaeusler.lsm4k.async.executor.AsyncTaskManager
import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor
import io.github.martinhaeusler.lsm4k.impl.EngineState
import io.github.martinhaeusler.lsm4k.util.ExceptionUtils
import io.github.oshai.kotlinlogging.KotlinLogging

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
     * - If the [engineState] is [EngineState.SHUTTING_DOWN] then the [exception]
     *   is very likely caused by an interrupt which was triggered by the shutdown of the
     *   scheduler. Some exceptions according to [ExceptionUtils.isIgnoredDuringShutdown] will be ignored.
     *   All other exceptions will be listed as warnings.
     *
     * - If the [engineState] is **not** [EngineState.SHUTTING_DOWN], any exception
     *   will be logged and re-thrown.
     *
     * Individual implementations may override this behavior.
     *
     * @param exception The exception that has been thrown by the [run] method of this task.
     * @param engineState The current state of the store.
     */
    fun handleTaskException(exception: Exception, engineState: EngineState) {
        if (engineState != EngineState.SHUTTING_DOWN) {
            log.error(exception) {
                "A fatal exception has occurred during the execution of the asynchronous task '${this.name}'" +
                    " of type '${this::class.qualifiedName}' on thread '${Thread.currentThread().name}'." +
                    " This may cause deadlocks and/or starvation. The exception was: ${exception}"
            }
            throw exception
        }
        if (ExceptionUtils.isIgnoredDuringShutdown(exception)) {
            // these exceptions are normal behavior during shutdown and don't need to be logged.
            return
        } else {
            // most likely, these exceptions are not harmful because we're shutting down anyway. Log them.
            log.warn(exception) {
                "An unexpected exception occurred in task '${name}' during LSM4K Database Engine shutdown: ${exception}"
            }
        }
    }

}