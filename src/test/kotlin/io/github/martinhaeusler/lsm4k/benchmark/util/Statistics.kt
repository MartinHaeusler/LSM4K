package io.github.martinhaeusler.lsm4k.benchmark.util

import io.github.martinhaeusler.lsm4k.benchmark.util.NumberUtils.format
import kotlin.math.max
import kotlin.math.pow
import kotlin.math.sqrt

class Statistics {

    val min: Double

    val max: Double

    val sum: Double

    val average: Double

    val standardDeviation: Double

    val median: Double

    val dataPoints: Int

    val percentile5: Double

    val percentile25: Double

    val percentile75: Double

    val percentile95: Double

    val percentile99: Double

    companion object {

        fun <T : Number> Iterator<T>.statistics(): Statistics {
            return this.asSequence().statistics()
        }

        fun <T : Number> Sequence<T>.statistics(): Statistics {
            return Statistics(this.map { it.toDouble() }.toList())
        }

        fun <T : Number> Iterable<T>.statistics(): Statistics {
            return Statistics(this.map { it.toDouble() })
        }

    }

    constructor(data: Collection<Double>) {
        require(data.isNotEmpty()) { "Precondition violation - argument 'data' must not be empty!" }
        this.dataPoints = data.size
        val sortedData = data.sorted()
        this.min = sortedData.first()
        this.max = sortedData.last()
        this.median = sortedData[sortedData.size / 2]
        this.percentile5 = sortedData[(sortedData.size * 0.05).toInt()]
        this.percentile25 = sortedData[(sortedData.size * 0.25).toInt()]
        this.percentile75 = sortedData[(sortedData.size * 0.75).toInt()]
        this.percentile95 = sortedData[(sortedData.size * 0.95).toInt()]
        this.percentile99 = sortedData[(sortedData.size * 0.99).toInt()]
        this.sum = data.sum()
        this.average = data.average()
        this.standardDeviation = sqrt(data.fold(0.0) { accumulator, next -> accumulator + (next - this.average).pow(2.0) } / this.dataPoints)
    }

    fun prettyPrint(name: String): String {
        val namePart = if (name.isBlank()) {
            ""
        } else {
            ": ${name.trim()}"
        }
        val title = "Statistics${namePart}"
        val separatorLength = max(30, title.length+1)
        val doubleSeparator = "=".repeat(separatorLength)
        val singleSeparator = "-".repeat(separatorLength)
        return """
            |${doubleSeparator}
            |${title}
            |${singleSeparator}
            |    #: ${this.dataPoints}
            |    Min: ${this.min.format("%.3f")}
            |    Max: ${this.max.format("%.3f")}
            |    Avg: ${this.average.format("%.3f")}
            |    StD: ${this.standardDeviation.format("%.3f")}
            |    p05: ${this.percentile5.format("%.3f")}
            |    p25: ${this.percentile25.format("%.3f")}
            |    p50: ${this.median.format("%.3f")}
            |    p75: ${this.percentile75.format("%.3f")}
            |    p95: ${this.percentile95.format("%.3f")}
            |    p99: ${this.percentile95.format("%.3f")}
            |${doubleSeparator}
        """.trimMargin("|")
    }

    override fun toString(): String {
        return "Statistics[" +
            "min: ${min}, " +
            "max: ${max}, " +
            "avg: ${average}, " +
            "stdDev: ${standardDeviation}, " +
            "p5: ${percentile5}, " +
            "p25: ${percentile25}, " +
            "p50: ${median}, " +
            "p75: ${percentile75}, " +
            "p95: ${percentile95}, " +
            "p99: ${percentile99}, " +
            "sample size: ${dataPoints}" +
            "]"
    }

}