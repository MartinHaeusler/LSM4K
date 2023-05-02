package org.chronos.chronostore.util.statistics

import com.google.common.math.Quantiles
import kotlin.math.pow
import kotlin.math.sqrt

object StatisticExtensions {

    fun <T> List<T>.median(): Double where T : Number, T : Comparable<T> {
        if (this.isEmpty()) {
            return 0.0
        }
        return this.sorted().let { Quantiles.median().compute(this) }
    }


    fun <T> Sequence<T>.median(): Double where T : Number, T : Comparable<T> {
        return this.toList().median()
    }

    fun <T> List<T>.stdDev(): Double where T : Number, T : Comparable<T> {
        if (this.isEmpty()) {
            return 0.0
        }
        val median = this.median()
        val denominator = this.size - 1
        return sqrt(this.sumOf { (it.toDouble() - median).pow(2) } / denominator)
    }

    fun <T> Sequence<T>.stdDev(): Double where T: Number, T: Comparable<T> {
        return this.toList().stdDev()
    }

}