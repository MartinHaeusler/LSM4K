package org.chronos.chronostore.impl

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class Killswitch(
    private val onTrigger: (message: String, cause: Throwable?) -> Unit,
) {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    private var triggered: Boolean = false

    private val lock = ReentrantLock()

    fun panic(message: String, cause: Throwable?) = this.lock.withLock {
        if (this.triggered) {
            log.warn(cause) { "Killswitch was triggered another time (which has no effect). Message: '${message}', cause: ${cause}" }
            return
        }
        this.triggered = true
        this.onTrigger(message, cause)
    }

}