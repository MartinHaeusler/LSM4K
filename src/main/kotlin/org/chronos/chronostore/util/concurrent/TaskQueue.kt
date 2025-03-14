package org.chronos.chronostore.util.concurrent

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitorImpl
import org.chronos.chronostore.async.tasks.AsyncTask
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.time.Duration

class TaskQueue(
    private val executor: Executor,
) {

    private val queue = ArrayDeque<AsyncTask>()

    private val lock = ReentrantLock(true)

    private var executingTask: AsyncTask? = null
    private var executingTaskMonitor: TaskMonitor? = null

    private val queueIsEmpty = lock.newCondition()

    fun addTask(task: AsyncTask) {
        this.lock.withLock {
            this.queue.add(task)
            this.deployNextTaskIfIdle()
        }
    }


    fun clearWaitingTasksIf(predicate: (AsyncTask) -> Boolean) {
        this.lock.withLock {
            this.queue.removeIf(predicate)
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

            this.executor.execute {
                try {
                    nextTask.run(taskMonitor)
                } finally {
                    this.lock.withLock {
                        this.executingTask = null
                        this.executingTaskMonitor = null
                        if (this.queue.isEmpty()) {
                            // queue has become empty, let all waiting threads know.
                            this.queueIsEmpty.signalAll()
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
                this.queueIsEmpty.await(remainingWaitTime, TimeUnit.MILLISECONDS)
            }
            return this.queue.isEmpty()
        }
    }

    fun waitUntilEmpty() {
        this.lock.withLock {
            while (!this.queue.isEmpty()) {
                this.queueIsEmpty.await()
            }
        }
    }

}