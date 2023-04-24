package org.chronos.chronostore.async.executor

import org.chronos.chronostore.async.tasks.AsyncTask
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import kotlin.time.Duration

interface AsyncTaskManager : AutoCloseable {

    fun executeAsync(task: AsyncTask): Future<TaskExecutionResult>

    fun scheduleRecurringWithTimeBetweenExecutions(task: AsyncTask, timeBetweenExecutions: Duration)


}