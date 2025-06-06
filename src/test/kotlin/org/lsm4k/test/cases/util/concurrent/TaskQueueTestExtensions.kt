package org.lsm4k.test.cases.util.concurrent

import org.junit.jupiter.api.fail
import org.lsm4k.util.concurrent.TaskQueue
import kotlin.time.Duration

object TaskQueueTestExtensions {

    fun TaskQueue.waitUntilEmptyOrFail(duration: Duration){
        val success = this.waitUntilEmpty(duration)
        if(!success){
            fail("Task queue did not become empty after waiting ${duration}!")
        }
    }

}