package org.lsm4k.util.logging

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.Level


internal object LogExtensions {

    inline fun KLogger.perfTrace(crossinline message: () -> String) {
        if (!this.isTraceEnabled()){
            return
        }
        this.at(Level.TRACE, LogMarkers.PERFORMANCE) {
            this.message = message()
        }
    }

    inline fun KLogger.ioDebug(crossinline message: () -> String) {
        if (!this.isDebugEnabled()){
            return
        }
        this.at(Level.DEBUG, LogMarkers.IO) {
            this.message = message()
        }
    }


}