package io.github.martinhaeusler.lsm4k.benchmark

import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.comparator.UnsignedBytesComparator
import io.github.martinhaeusler.lsm4k.util.unit.SizeUnit
import kotlin.random.Random
import kotlin.system.measureTimeMillis

object BytesComparisonBenchmark {

    @JvmStatic
    fun main(args: Array<String>) {
        val random = Random(12345)
        val bigBytes = Bytes.wrap(random.nextBytes(SizeUnit.MEBIBYTE.toBytes(8).toInt()))

        var result = 0
        repeat(100){
            result += bigBytes.compareTo(bigBytes)
        }
        val time = measureTimeMillis {
            repeat(10_000){
                result += bigBytes.compareTo(bigBytes)
            }
        }
        println("Comparator: ${UnsignedBytesComparator.BEST_COMPARATOR}")
        println("Result: ${result}")
        println("Time: ${time}ms")
    }

}