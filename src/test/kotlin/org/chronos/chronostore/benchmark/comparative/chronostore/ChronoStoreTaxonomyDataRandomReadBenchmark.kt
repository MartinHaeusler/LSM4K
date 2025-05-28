package org.chronos.chronostore.benchmark.comparative.chronostore

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.TransactionalStore.Companion.withCursor
import org.chronos.chronostore.benchmark.util.Statistics.Companion.statistics
import org.chronos.chronostore.util.bytes.BasicBytes
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.unit.BinarySize.Companion.GiB
import java.io.File
import kotlin.system.measureTimeMillis

object ChronoStoreTaxonomyDataRandomReadBenchmark {

    private const val REPETITIONS = 10
    private const val NUMBER_OF_READS = 1000

    @JvmStatic
    fun main(args: Array<String>) {
        println("STARTING BENCHMARK")
        val inputDir = File("/home/martin/Documents/chronostore-test/taxonomyChronoStore")

        var timeBeforeRead = -1L
        var blackHole = 0L
        val dataPoints = ArrayList<Long>(REPETITIONS)

        // get all keys in the store
        val allKeys = mutableListOf<Bytes>()
        val configuration = ChronoStoreConfiguration(
            blockCacheSize = 4.GiB,
        )
        val statisticsReport = ChronoStore.openOnDirectory(inputDir, configuration).use { chronoStore ->
            measureTimeMillis {
                chronoStore.readWriteTransaction { tx ->
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
                    chronoStore.readWriteTransaction { tx ->
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

            chronoStore.statisticsReport()
        }

        val timeAfterRead = System.currentTimeMillis()
        val totalTime = timeAfterRead - timeBeforeRead
        println("Black hole: ${blackHole}")
        println("Total Time: ${totalTime}ms")
        println(dataPoints.statistics().prettyPrint("ChronoStore read ${NUMBER_OF_READS} keys (${REPETITIONS} repetitions)"))
        println()
        println()
        println()
        println(statisticsReport.prettyPrint())
    }

}