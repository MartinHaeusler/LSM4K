package org.chronos.chronostore.benchmark.comparative.chronostore

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ByteIterator
import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.benchmark.util.Statistics.Companion.statistics
import org.chronos.chronostore.util.bytes.BasicBytes
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import org.chronos.chronostore.util.unit.MiB
import java.io.File
import java.io.InputStream
import kotlin.system.measureTimeMillis

object ChronoStoreTaxonomyDataRandomReadBenchmark {

    val REPETITIONS = 1000
    val NUMBER_OF_READS = 1000

    @JvmStatic
    fun main(args: Array<String>) {
        println("STARTING BENCHMARK")
        val inputDir = File("/home/martin/Documents/chronostore-test/taxonomyChronoStore")

        var timeBeforeRead = -1L
        var blackHole = 0L
        val dataPoints = ArrayList<Long>(REPETITIONS)

        // get all keys in the store
        val allKeys = mutableListOf<Bytes>()
        val configuration = ChronoStoreConfiguration()
        configuration.blockCacheSize = 2000.MiB
        ChronoStore.openOnDirectory(inputDir, configuration).use { chronoStore ->
            chronoStore.transaction { tx ->
                val store = tx.getStore("data")
                store.openCursorOnLatest().use { cursor ->
                    cursor.firstOrThrow()
                    allKeys += cursor.ascendingKeySequenceFromHere()
                }
            }
            println("There are ${allKeys.size} unique keys in the store.")
            val keyList = allKeys.toList()
            val bullshitKey = BasicBytes("bullshit")

            timeBeforeRead = System.currentTimeMillis()
            repeat(REPETITIONS) { repetitionIndex ->
                measureTimeMillis {
                    chronoStore.transaction { tx ->
                        val store = tx.getStore("data")
                        repeat(NUMBER_OF_READS) { readIndex ->
                            val keyToRead = if ((repetitionIndex + readIndex) % 100 == 0) {
                                bullshitKey
                            } else {
                                keyList.random()
                            }

                            val value = store.getLatest(keyToRead)
                            blackHole += value?.size ?: 1
                        }
                    }
                }.let(dataPoints::add)
            }
        }

        val timeAfterRead = System.currentTimeMillis()
        val totalTime = timeAfterRead - timeBeforeRead
        println("Black hole: ${blackHole}")
        println("Total Time: ${totalTime}ms")
        println(dataPoints.statistics().prettyPrint("ChronoStore read ${NUMBER_OF_READS} keys (${REPETITIONS} repetitions)"))
        println()
        println()
        println()
        println(ChronoStoreStatistics.snapshot().prettyPrint())
    }

}