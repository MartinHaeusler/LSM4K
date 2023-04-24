package org.chronos.chronostore.async.executor

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.tasks.AsyncTask
import java.util.concurrent.*
import kotlin.time.Duration

class AsyncTaskManagerImpl(
    private val executorService: ScheduledExecutorService,
) : AsyncTaskManager {

    override fun executeAsync(task: AsyncTask): Future<TaskExecutionResult> {
        return this.executorService.submit(createCallableForTask(task))
    }

    override fun scheduleRecurringWithTimeBetweenExecutions(task: AsyncTask, timeBetweenExecutions: Duration) {
        val delayMillis = timeBetweenExecutions.inWholeMilliseconds
        this.executorService.scheduleWithFixedDelay({
            this.createCallableForTask(task).call()
        }, delayMillis, delayMillis, TimeUnit.MILLISECONDS)
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
}