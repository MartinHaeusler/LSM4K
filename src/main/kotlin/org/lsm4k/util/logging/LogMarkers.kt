package org.lsm4k.util.logging

import io.github.oshai.kotlinlogging.KMarkerFactory

object LogMarkers {

    val IO = KMarkerFactory.getMarker("io")

    val PERFORMANCE = KMarkerFactory.getMarker("performance")


}