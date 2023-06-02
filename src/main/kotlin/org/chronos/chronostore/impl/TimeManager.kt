package org.chronos.chronostore.impl

import org.chronos.chronostore.util.Timestamp
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class TimeManager(
    startTimestamp: Timestamp
) {

    private var lastReturnedTimestamp = -1L

    private val lock = ReentrantLock(true)

    init {
        require(startTimestamp >= 0) { "Argument 'startTimestamp' (${startTimestamp}) must not be negative!" }
        this.lastReturnedTimestamp = startTimestamp
    }

    /**
     * Gets a wall-clock timestamp which is unique (i.e. each timestamp is only returned once by this function).
     *
     * If the current timestamp has already been consumed, the current thread will wait for the next available
     * timestamp, which will then be returned immediately.
     *
     * @return The current system timestamp. Each timestamp will only be returned once.
     */
    fun getUniqueWallClockTimestamp(): Timestamp {
        this.lock.withLock {
            var time = System.currentTimeMillis()
            while (time <= lastReturnedTimestamp) {
                Thread.sleep(1)
                time = System.currentTimeMillis()
            }
            lastReturnedTimestamp = time
            return time
        }
    }

    fun getNow(): Timestamp {
        return this.lastReturnedTimestamp
    }

}