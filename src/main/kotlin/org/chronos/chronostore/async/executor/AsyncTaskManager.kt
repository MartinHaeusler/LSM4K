package org.chronos.chronostore.async.executor

import org.chronos.chronostore.async.tasks.AsyncTask
import java.util.concurrent.Future

interface AsyncTaskManager: AutoCloseable {

    fun execute(task: AsyncTask): Future<TaskExecutionResult>



}