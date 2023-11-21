package org.chronos.chronostore.benchmark.util

object NumberUtils {

    fun Double.format(format: String): String {
        return String.format(format, this)
    }

}