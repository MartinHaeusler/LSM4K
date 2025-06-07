package io.github.martinhaeusler.lsm4k.impl

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.Volatile
import kotlin.concurrent.withLock

class Killswitch(
    private val onTrigger: (message: String, cause: Throwable?) -> Unit,
) {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    @Volatile
    private var triggered: Boolean = false

    @Volatile
    private var enabled: Boolean = true

    private val lock = ReentrantLock()

    fun panic(message: String, cause: Throwable?) = this.lock.withLock {
        if (!this.enabled) {
            log.debug { "Killswitch was triggered, but is disabled. Message: '${message}', cause: ${cause}" }
            return
        }
        if (this.triggered) {
            log.warn(cause) { "Killswitch was triggered another time (which has no effect). Message: '${message}', cause: ${cause}" }
            return
        }
        this.triggered = true
        this.onTrigger(message, cause)
    }

    fun disable() {
        if (!this.enabled) {
            return
        }
        this.enabled = false
        log.debug { "Killswitch disabled." }
    }

}