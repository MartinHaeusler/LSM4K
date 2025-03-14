package org.chronos.chronostore.util.concurrent

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitorImpl
import org.chronos.chronostore.async.tasks.AsyncTask
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.time.Duration

class TaskQueue(
    private val executor: Executor,
) {

    private val queue = ArrayDeque<AsyncCompletableTask>()

    private val lock = ReentrantLock(true)

    private var executingTask: AsyncTask? = null
    private var executingTaskMonitor: TaskMonitor? = null

    private val queueIsEmptyCondition = lock.newCondition()

    fun schedule(task: AsyncTask): CompletableFuture<*> {
        this.lock.withLock {
            val completableTask = AsyncCompletableTask(task)
            this.queue.add(completableTask)
            this.deployNextTaskIfIdle()
            return completableTask.future
        }
    }


    fun cancelWaitingTasksIf(predicate: (AsyncTask) -> Boolean) {
        this.lock.withLock {
            if (this.queue.isEmpty()) {
                return
            }
            val iterator = this.queue.iterator()
            var changed = false
            while (iterator.hasNext()) {
                val currentTask = iterator.next()
                if (!predicate(currentTask.innerTask)) {
                    // predicate doesn't match the task, leave it be.
                    continue
                }
                // cancel this task and remove it from the queue
                currentTask.cancel()
                iterator.remove()
                changed = true
            }
            if (changed && this.queue.isEmpty() && this.executingTask == null) {
                // queue became empty because of this operation
                this.queueIsEmptyCondition.signalAll()
            }
        }
    }

    fun cancelWaitingTasks() {
        this.lock.withLock {
            if (this.queue.isEmpty()) {
                return
            }

            for (task in this.queue) {
                task.cancel()
            }
            this.queue.clear()
            if (this.executingTask == null) {
                // queue became empty because of this operation
                this.queueIsEmptyCondition.signalAll()
            }
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

            this.executor.execute {
                try {
                    nextTask.run(taskMonitor)
                } finally {
                    this.lock.withLock {
                        this.executingTask = null
                        this.executingTaskMonitor = null
                        if (this.queue.isEmpty()) {
                            // queue has become empty, let all waiting threads know.
                            this.queueIsEmptyCondition.signalAll()
                        } else {
                            // queue is not empty, proceed by scheduling the next task.
                            this.deployNextTaskIfIdle()
                        }
                    }
                }
            }
        }
    }

    fun waitUntilEmpty(duration: Duration): Boolean {
        this.lock.withLock {
            val maxWait = System.currentTimeMillis() + duration.inWholeMilliseconds
            while (!this.queue.isEmpty()) {
                val remainingWaitTime = maxWait - System.currentTimeMillis()
                if (remainingWaitTime <= 0) {
                    return false
                }
                this.queueIsEmptyCondition.await(remainingWaitTime, TimeUnit.MILLISECONDS)
            }
            return true
        }
    }

    fun waitUntilEmpty() {
        this.lock.withLock {
            while (!this.queue.isEmpty()) {
                this.queueIsEmptyCondition.await()
            }
        }
    }

    private class AsyncCompletableTask(
        val innerTask: AsyncTask,
    ) : AsyncTask {

        val future: CompletableFuture<*> = CompletableFuture<Void>()

        override val name: String
            get() = this.innerTask.name

        override fun run(monitor: TaskMonitor) {
            try {
                this.innerTask.run(monitor)
                this.future.complete(null)
            } catch (ex: Throwable) {
                this.future.completeExceptionally(ex)
            }
        }

        fun cancel() {
            this.future.cancel(true)
        }

    }
}