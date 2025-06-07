package io.github.martinhaeusler.lsm4k.test.cases.util.concurrent

import io.github.martinhaeusler.lsm4k.util.concurrent.TaskQueue
import org.junit.jupiter.api.fail
import kotlin.time.Duration

object TaskQueueTestExtensions {

    fun TaskQueue.waitUntilEmptyOrFail(duration: Duration){
        val success = this.waitUntilEmpty(duration)
        if(!success){
            fail("Task queue did not become empty after waiting ${duration}!")
        }
    }

}