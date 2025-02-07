package org.chronos.chronostore.async.executor

import com.cronutils.model.Cron
import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.impl.ChronoStoreState
import org.chronos.chronostore.util.cron.CronUtils.isValid
import org.chronos.chronostore.util.cron.CronUtils.nextExecution
import java.util.concurrent.Callable
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import kotlin.time.Duration

class AsyncTaskManagerImpl(
    private val executorService: ScheduledExecutorService,
    private val getChronoStoreState: () -> ChronoStoreState,
) : AsyncTaskManager {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    override fun executeAsync(task: AsyncTask): Future<TaskExecutionResult> {
        return this.executorService.submit(createCallableForTask(task))
    }

    override fun scheduleRecurringWithTimeBetweenExecutions(task: AsyncTask, timeBetweenExecutions: Duration) {
        val delayMillis = timeBetweenExecutions.inWholeMilliseconds
        this.executorService.scheduleWithFixedDelay({
            this.createCallableForTask(task).call()
        }, delayMillis, delayMillis, TimeUnit.MILLISECONDS)
    }

    override fun scheduleRecurringWithFixedRate(task: AsyncTask, initialDelay: Duration, delayBetweenExecutions: Duration) {
        this.executorService.scheduleAtFixedRate(
            /* command = */ { this.createCallableForTask(task).call() },
            /* initialDelay = */ initialDelay.inWholeMilliseconds,
            /* period = */ delayBetweenExecutions.inWholeMilliseconds,
            /* unit = */ TimeUnit.MILLISECONDS
        )
    }

    override fun scheduleRecurringWithCron(task: AsyncTask, cron: Cron) {
        require(cron.isValid()) { "Cron expression '${cron}' is invalid!" }
        val recurringTask = RecurringCronTask(task, cron, this.executorService)
        val nextExecution = recurringTask.cron.nextExecution()
        if (nextExecution == null) {
            // don't execute
            log.warn { "Cannot schedule task '${task.name}' with cron '${cron.asString()}' because the cron expression has no next execution date!" }
            return
        }
        log.debug {
            "[SCHEDULING] Scheduling of task '${task.name}' has been initialized with cron '${cron.asString()}'." +
                " First execution will be at: ${nextExecution.nextDateTime} (delay: ${nextExecution.delayInMillis}ms)"
        }
        this.executorService.schedule(createCallableForTask(recurringTask), nextExecution.delayInMillis, TimeUnit.MILLISECONDS)
    }

    private fun createCallableForTask(task: AsyncTask): Callable<TaskExecutionResult> {
        return Callable {
            val startTime = System.currentTimeMillis()
            val monitor = TaskMonitor.create()
            try {
                task.run(monitor)
            } catch (e: Exception) {
                if (!monitor.status.state.isTerminal) {
                    monitor.reportFailed(e)
                }
                task.handleTaskException(e, this.getChronoStoreState())
            } finally {
                if (!monitor.status.state.isTerminal) {
                    monitor.reportDone()
                }
            }
            val endTime = System.currentTimeMillis()
            val monitorStatus = monitor.status
            val result = when (monitorStatus.state) {
                TaskMonitor.State.INITIAL, TaskMonitor.State.IN_PROGRESS -> throw IllegalStateException("TaskMonitor ended in invalid state: ${monitorStatus.state}")
                TaskMonitor.State.FAILED -> TaskExecutionResult.TaskExecutionFailed(startTime, endTime, monitorStatus.failureMessage, monitorStatus.failureCause)
                TaskMonitor.State.SUCCEEDED -> TaskExecutionResult.TaskExecutionSuccessful(startTime, endTime)
            }
            result
        }
    }

    override fun close() {
        this.executorService.shutdownNow()
    }


    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    inner class RecurringCronTask(
        val task: AsyncTask,
        val cron: Cron,
        private val executorService: ScheduledExecutorService,
    ) : AsyncTask {

        override val name: String
            get() = this.task.name

        override fun run(monitor: TaskMonitor) {
            try {
                this.task.run(monitor)
            } finally {
                // never reschedule during system shutdown
                val state = this@AsyncTaskManagerImpl.getChronoStoreState()
                if (state != ChronoStoreState.SHUTTING_DOWN) {
                    rescheduleTaskForNextRun()
                }
            }
        }

        override fun handleTaskException(exception: Exception, chronoStoreState: ChronoStoreState) {
            // forward the call to the nested task
            this.task.handleTaskException(exception, chronoStoreState)
        }

        private fun rescheduleTaskForNextRun() {
            // the task has been executed, reschedule it
            val next = cron.nextExecution()
            if (next != null) {
                // only execute again if we have a non-null, non-negative duration towards the next execution.
                log.debug { "[SCHEDULING] Task '${this.name}' has been executed. Rescheduling for cron '${this.cron.asString()}'. Next execution will be at: ${next.nextDateTime}" }
                val newCallable = createCallableForTask(this)
                this.executorService.schedule(newCallable, next.delayInMillis, TimeUnit.MILLISECONDS)
            } else {
                // the cron expression produced no next execution date. This means we can never execute this task again!
                log.warn { "[SCHEDULING] Task '${this.name}' has been executed. Rescheduling for cron '${this.cron.asString()}' produced no next execution date; no further executions will be performed!" }
            }
        }

        override fun toString(): String {
            return "CronTask[${this.name}, ${this.cron}]"
        }
    }
}