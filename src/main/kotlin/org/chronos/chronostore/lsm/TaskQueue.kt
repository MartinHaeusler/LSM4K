package org.chronos.chronostore.lsm

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitorImpl
import org.chronos.chronostore.async.tasks.AsyncTask
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class TaskQueue(
    private val executor: ExecutorService,
) {

    private val queue = ArrayDeque<AsyncTaskWithFuture>()

    private val lock = ReentrantLock(true)

    private var executingTask: AsyncTaskWithFuture? = null
    private var executingTaskMonitor: TaskMonitor? = null

    fun addTask(task: AsyncTask): Future<*> {
        this.lock.withLock {
            val taskWithFuture = AsyncTaskWithFuture(task)
            this.queue.add(taskWithFuture)
            this.deployNextTaskIfIdle()
            return taskWithFuture.future
        }
    }


    fun clearWaitingTasksIf(predicate: (AsyncTask) -> Boolean) {
        this.lock.withLock {
            this.queue.removeIf { predicate(it.innerTask) }
        }
    }

    fun clearWaitingTasks() {
        this.lock.withLock {
            this.queue.clear()
        }
    }

    private fun deployNextTaskIfIdle() {
        this.lock.withLock {
            if (this.executingTask != null) {
                // task is still running, skip.
                return
            }
            val nextTask = this.queue.removeFirstOrNull()
                ?: return // no tasks waiting in the queue

            val taskMonitor = TaskMonitorImpl()

            this.executingTask = nextTask
            this.executingTaskMonitor = taskMonitor

            // when the task is done, attempt to fire the next one in the queue.
            nextTask.future.thenRun(this::deployNextTaskIfIdle)

            this.executor.submit {
                nextTask.run(taskMonitor)
            }
        }
    }

    private class AsyncTaskWithFuture(
        val innerTask: AsyncTask,
    ) : AsyncTask {

        val future = CompletableFuture<Unit>()

        override val name: String
            get() = this.innerTask.name

        override fun run(monitor: TaskMonitor) {
            try {
                this.innerTask.run(monitor)
                this.future.complete(Unit)
            } catch (e: Exception) {
                this.future.completeExceptionally(e)
            }
        }

        override fun toString(): String {
            return this.innerTask.toString()
        }

    }

}