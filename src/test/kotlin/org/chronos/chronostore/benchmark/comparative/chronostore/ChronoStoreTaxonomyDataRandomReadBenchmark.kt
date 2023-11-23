package org.chronos.chronostore.benchmark.comparative.chronostore

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ByteIterator
import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.benchmark.util.Statistics.Companion.statistics
import org.chronos.chronostore.util.bytes.BasicBytes
import org.chronos.chronostore.util.bytes.Bytes
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
        val allKeys = mutableSetOf<Bytes>()
        ChronoStore.openOnDirectory(inputDir).use { chronoStore ->
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
                println(repetitionIndex)
            }
        }

        val timeAfterRead = System.currentTimeMillis()
        val totalTime = timeAfterRead - timeBeforeRead
        println("Black hole: ${blackHole}")
        println("Total Time: ${totalTime}ms")
        println(dataPoints.statistics().prettyPrint("ChronoStore read ${NUMBER_OF_READS} keys (${REPETITIONS} repetitions)"))
    }

    private class XodusByteInputStream(
        private val iterator: ByteIterator
    ) : InputStream() {

        constructor(iterable: ByteIterable) : this(iterable.iterator())

        override fun read(): Int {
            if (!this.iterator.hasNext()) {
                return -1
            }
            // to convert negative byte values to positive
            // integers, we perform the "and 0xff". If we
            // don't do this, we get negative integers, which
            // is not what we want.
            return this.iterator.next().toInt() and 0xff
        }

    }

}