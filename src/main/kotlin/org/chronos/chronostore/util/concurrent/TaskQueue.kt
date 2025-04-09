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

/**
 * A simple task queue implementation which allows to [schedule] tasks.
 *
 * Only **one** task at a time will be executed. The number of tasks in the
 * queue is unbounded.
 *
 * The actual execution itself is deferred to the given [executor].
 */
class TaskQueue(
    /** The executor responsible for actual task execution. */
    private val executor: Executor,
) {

    /** The waiting queue. */
    private val queue = ArrayDeque<AsyncCompletableTask>()

    /** The lock we use to ensure atomicity of operations. */
    private val lock = ReentrantLock(true)

    /** The currently executing task, or `null` if the task queue is idle.*/
    private var executingTask: AsyncTask? = null

    /** The [TaskMonitor]  which tracks the currently executing task, or `null` if the task queue is idle.*/
    private var executingTaskMonitor: TaskMonitor? = null

    /** A condition which threads can wait for. Will fire when the queue becomes empty. */
    private val queueIsEmptyCondition = lock.newCondition()

    /**
     * Schedules the given [task] for execution at the next possible point in time.
     *
     * Only one task will be executed at the same time.
     *
     * @param task The task to be executed
     *
     * @return a [CompletableFuture] which can be used to track the task status.
     */
    fun schedule(task: AsyncTask): CompletableFuture<*> {
        this.lock.withLock {
            val completableTask = AsyncCompletableTask(task)
            this.queue.add(completableTask)
            this.deployNextTaskIfIdle()
            return completableTask.future
        }
    }

    /**
     * Allows to atomically cancel one or more tasks waiting in the queue.
     *
     * @param predicate The predicate which tells us if a given task should be cancelled or not.
     */
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

    /**
     * Cancels all waiting tasks unconditionally.
     */
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

    /**
     * Called when a task has completed its execution. Starts the next task waiting in the [queue].
     */
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

    /**
     * Waits until either this queue becomes empty or the given [duration] has elapsed.
     *
     * This is a blocking operation.
     *
     * @param duration the maximum duration to wait for
     *
     * @return `true` if the task queue became empty **before** the [duration] expired.
     * `false` if the duration expired first (in which case, the task queue may still contain tasks).
     */
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

    /**
     * Waits until this queue becomes empty with no timeout.
     *
     * This is a blocking operation.
     */
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