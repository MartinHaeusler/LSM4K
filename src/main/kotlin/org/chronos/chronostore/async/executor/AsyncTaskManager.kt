package org.chronos.chronostore.async.executor

import com.cronutils.model.Cron
import org.chronos.chronostore.async.tasks.AsyncTask
import java.util.concurrent.Future
import kotlin.time.Duration

interface AsyncTaskManager : AutoCloseable {

    fun executeAsync(task: AsyncTask): Future<TaskExecutionResult>

    fun scheduleRecurringWithTimeBetweenExecutions(task: AsyncTask, timeBetweenExecutions: Duration)

    fun scheduleRecurringWithFixedRate(task: AsyncTask, initialDelay: Duration, delayBetweenExecutions: Duration)

    fun scheduleRecurringWithCron(task: AsyncTask, cron: Cron)

}