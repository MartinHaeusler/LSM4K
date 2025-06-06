package org.lsm4k.async.executor

import org.lsm4k.util.Timestamp
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

sealed interface TaskExecutionResult {

        val startTime: Timestamp
        val endTime: Timestamp
        val duration: Duration
            get() = (endTime - startTime).toDuration(DurationUnit.MILLISECONDS)

        class TaskExecutionSuccessful(
            override val startTime: Timestamp,
            override val endTime: Timestamp
        ) : TaskExecutionResult {
    
            init {
                require(startTime > 0) { "Argument 'startTime' (${startTime}) must be positive!" }
                require(endTime > 0) { "Argument 'startTime' (${endTime}) must be positive!" }
                require(startTime <= endTime) { "Argument 'startTime' (${startTime}) must be less than or equal to argument 'endTime' (${endTime})!" }
            }
    
        }
    
        class TaskExecutionFailed(
            override val startTime: Timestamp,
            override val endTime: Timestamp,
            val errorMessage: String?,
            val errorCause: Throwable?,
        ) : TaskExecutionResult {
    
            init {
                require(startTime > 0) { "Argument 'startTime' (${startTime}) must be positive!" }
                require(endTime > 0) { "Argument 'startTime' (${endTime}) must be positive!" }
                require(startTime <= endTime) { "Argument 'startTime' (${startTime}) must be less than or equal to argument 'endTime' (${endTime})!" }
            }
            
        }
        
    }