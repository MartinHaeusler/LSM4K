package org.chronos.chronostore.util.logging

import io.github.oshai.kotlinlogging.KMarkerFactory

object LogMarkers {

    val IO = KMarkerFactory.getMarker("io")

    val PERFORMANCE = KMarkerFactory.getMarker("performance")


}