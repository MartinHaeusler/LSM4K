package org.chronos.chronostore.test.cases.util.concurrent

import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.mainTask
import org.chronos.chronostore.async.tasks.AsyncTask
import org.chronos.chronostore.test.cases.util.concurrent.TaskQueueTestExtensions.waitUntilEmptyOrFail
import org.chronos.chronostore.util.concurrent.TaskQueue
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import java.util.concurrent.Executors
import kotlin.time.Duration.Companion.seconds

class TaskQueueTest {

    @Test
    fun atMostOneTaskIsExecutedAtTheSameTime() {
        Executors.newFixedThreadPool(4).use { feedingExecutor ->
            Executors.newFixedThreadPool(4).use { consumingExecutor ->
                val queue = TaskQueue(consumingExecutor)

                var value = 0
                repeat(1000) {
                    feedingExecutor.submit {
                        queue.schedule(object : AsyncTask {
                            override val name: String
                                get() = "Test task"

                            override fun run(monitor: TaskMonitor) {
                                monitor.mainTask("Incrementing") {
                                    value++
                                }
                            }
                        })
                    }
                }

                // by having 4 threads concurrently incrementing an integer
                // 1000 times, the chances that it really ends up at exactly
                // 1000 are extremely slim, unless there is exactly one thread
                // working at each given point in time.
                queue.waitUntilEmptyOrFail(60.seconds)

                expectThat(value).isEqualTo(1000)
            }
        }
    }

}