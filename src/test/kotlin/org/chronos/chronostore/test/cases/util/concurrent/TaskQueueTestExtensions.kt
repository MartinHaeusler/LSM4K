package org.chronos.chronostore.test.cases.util.concurrent

import org.chronos.chronostore.util.concurrent.TaskQueue
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