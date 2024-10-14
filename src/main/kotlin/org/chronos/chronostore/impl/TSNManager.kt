package org.chronos.chronostore.impl

import org.chronos.chronostore.util.TSN
import org.chronos.chronostore.util.Timestamp
import java.util.concurrent.atomic.AtomicLong

class TSNManager(
    startTSN: TSN,
) {

    private var generator: AtomicLong

    init {
        require(startTSN >= 0) { "Argument 'startTSN' (${startTSN}) must not be negative!" }
        this.generator = AtomicLong(startTSN)
    }

    /**
     * Gets a [TSN] which is unique (i.e. each [TSN] is only returned once by this function).
     *
     * @return The current [TSN]. Each [TSN] will only be returned once.
     */
    fun getUniqueTSN(): TSN {
        // it is CRITICAL that we increment FIRST and THEN get the value, because
        // we have to be able to report on the last returned TSN. Therefore, in
        // order to create a unique one, we cannot take the CURRENT one, but we
        // first have to generate a new one and THEN use that.
        return this.generator.incrementAndGet()
    }

    fun getLastReturnedTSN(): Timestamp {
        return this.generator.get()
    }

}