package org.chronos.chronostore.util.log

import mu.KLogger

object LogExtensions {

    inline fun KLogger.performance(message: () -> String) {
        if (this.isTraceEnabled) {
            this.trace(LogMarkers.PERFORMANCE, message())
        }
    }


}