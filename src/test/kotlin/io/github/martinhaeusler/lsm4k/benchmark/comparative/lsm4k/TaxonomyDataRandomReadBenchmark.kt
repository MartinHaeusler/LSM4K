package io.github.martinhaeusler.lsm4k.benchmark.comparative.lsm4k

import io.github.martinhaeusler.lsm4k.api.DatabaseEngine
import io.github.martinhaeusler.lsm4k.api.LSM4KConfiguration
import io.github.martinhaeusler.lsm4k.api.TransactionalStore.Companion.withCursor
import io.github.martinhaeusler.lsm4k.benchmark.util.Statistics.Companion.statistics
import io.github.martinhaeusler.lsm4k.util.bytes.BasicBytes
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.GiB
import java.io.File
import kotlin.system.measureTimeMillis

object TaxonomyDataRandomReadBenchmark {

    private const val REPETITIONS = 10
    private const val NUMBER_OF_READS = 1000

    @JvmStatic
    fun main(args: Array<String>) {
        println("STARTING BENCHMARK")
        val inputDir = File("/home/martin/Documents/lsm4k-test/taxonomyLSM4K")

        var timeBeforeRead = -1L
        var blackHole = 0L
        val dataPoints = ArrayList<Long>(REPETITIONS)

        // get all keys in the store
        val allKeys = mutableListOf<Bytes>()
        val configuration = LSM4KConfiguration(
            blockCacheSize = 4.GiB,
        )
        val statisticsReport = DatabaseEngine.openOnDirectory(inputDir, configuration).use { engine ->
            engine.statistics.startCollection()
            measureTimeMillis {
                engine.readWriteTransaction { tx ->
                    val store = tx.getStore("data")
                    store.withCursor { cursor ->
                        cursor.firstOrThrow()
                        allKeys += cursor.ascendingKeySequenceFromHere().map { it.own() }
                    }
                }
            }.also { println("There are ${allKeys.size} unique keys in the store. Calculation time: ${it}ms") }
            val keyList = allKeys.toList()
            val bullshitKey = BasicBytes("bullshit")

            timeBeforeRead = System.currentTimeMillis()
            repeat(REPETITIONS) { repetitionIndex ->
                measureTimeMillis {
                    engine.readWriteTransaction { tx ->
                        val store = tx.getStore("data")
                        repeat(NUMBER_OF_READS) { readIndex ->
                            val keyToRead = if ((repetitionIndex + readIndex) % 100 == 0) {
                                bullshitKey
                            } else {
                                keyList.random()
                            }

                            val value = store.get(keyToRead)
                            blackHole += value?.size ?: 1
                        }
                    }
                }.let {
                    dataPoints += it
                    println(it)
                }
            }

            engine.statistics.report()!!
        }

        val timeAfterRead = System.currentTimeMillis()
        val totalTime = timeAfterRead - timeBeforeRead
        println("Black hole: ${blackHole}")
        println("Total Time: ${totalTime}ms")
        println(dataPoints.statistics().prettyPrint("DatabaseEngine read ${NUMBER_OF_READS} keys (${REPETITIONS} repetitions)"))
        println()
        println()
        println()
        println(statisticsReport.prettyPrint())
    }

}