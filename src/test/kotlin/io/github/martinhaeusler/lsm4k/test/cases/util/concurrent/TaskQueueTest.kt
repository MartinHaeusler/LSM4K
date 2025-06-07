package io.github.martinhaeusler.lsm4k.test.cases.util.concurrent

import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor
import io.github.martinhaeusler.lsm4k.async.taskmonitor.TaskMonitor.Companion.mainTask
import io.github.martinhaeusler.lsm4k.async.tasks.AsyncTask
import io.github.martinhaeusler.lsm4k.test.cases.util.concurrent.TaskQueueTestExtensions.waitUntilEmptyOrFail
import io.github.martinhaeusler.lsm4k.util.concurrent.TaskQueue
import org.awaitility.Awaitility
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds

class TaskQueueTest {

    @Test
    fun atMostOneTaskIsExecutedAtTheSameTime() {
        Executors.newFixedThreadPool(4).use { feedingExecutor ->
            Executors.newFixedThreadPool(4).use { consumingExecutor ->
                val queue = TaskQueue(consumingExecutor)

                var value = 0
                var submitted = 0
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
                        submitted++
                    }
                }

                Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until { submitted == 1000 }

                queue.waitUntilEmptyOrFail(60.seconds)

                // by having 4 threads concurrently incrementing an integer
                // 1000 times, the chances that it really ends up at exactly
                // 1000 are extremely slim, unless there is exactly one thread
                // working at each given point in time.
                expectThat(value).isEqualTo(1000)
            }
        }
    }

}