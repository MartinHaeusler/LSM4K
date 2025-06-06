package org.lsm4k.async.tasks

import org.lsm4k.async.taskmonitor.TaskMonitor
import java.util.concurrent.CompletableFuture

class CompletableAsyncTask(
    private val task: AsyncTask,
) : AsyncTask {

    val future: CompletableFuture<*> = CompletableFuture<Unit>()

    override val name: String
        get() = this.task.name

    override fun run(monitor: TaskMonitor) {
        try {
            this.task.run(monitor)
            this.future.complete(null)
        } catch (e: Throwable) {
            this.future.completeExceptionally(e)
            throw e
        }
    }

}