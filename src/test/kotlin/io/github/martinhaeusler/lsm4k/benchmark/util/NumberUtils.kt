package io.github.martinhaeusler.lsm4k.benchmark.util

object NumberUtils {

    fun Double.format(format: String): String {
        return String.format(format, this)
    }

}