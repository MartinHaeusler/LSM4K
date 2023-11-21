package org.chronos.chronostore.benchmark.comparative.xodus

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ByteIterator
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.StoreConfig
import org.chronos.chronostore.benchmark.comparative.xodus.XodusUtils.toBytes
import org.chronos.chronostore.benchmark.util.Statistics.Companion.statistics
import org.chronos.chronostore.util.bytes.Bytes
import java.io.File
import java.io.InputStream
import java.util.ArrayList
import kotlin.system.measureTimeMillis

object XodusTaxonomyDataRandomReadBenchmark {

    val REPETITIONS = 1000
    val NUMBER_OF_READS = 1000

    @JvmStatic
    fun main(args: Array<String>) {
        println("STARTING BENCHMARK")
        val inputDir = File("/home/martin/Documents/chronostore-test/taxonomyXodusBTree")

        var timeBeforeRead = -1L
        var blackHole = 0L
        val dataPoints = ArrayList<Long>(REPETITIONS)

        // get all keys in the store
        val allKeys = mutableSetOf<Bytes>()
        Environments.newInstance(inputDir).use { environment ->
            environment.computeInReadonlyTransaction { tx ->
                val store = environment.openStore("data", StoreConfig.USE_EXISTING, tx)
                store.openCursor(tx).use { cursor ->
                    while (cursor.next) {
                        allKeys += cursor.key.toBytes()
                    }
                }
            }
            println("There are ${allKeys.size} unique keys in the store.")
            val keyList = allKeys.asSequence().map { ArrayByteIterable(it.toSharedArray()) }.toList()

            val bullshitKey = ArrayByteIterable("bullshit".toByteArray())

            timeBeforeRead = System.currentTimeMillis()
            repeat(REPETITIONS) { repetitionIndex ->
                measureTimeMillis {
                    environment.computeInReadonlyTransaction { tx ->
                        val store = environment.openStore("data", StoreConfig.USE_EXISTING, tx)
                        repeat(NUMBER_OF_READS) { readIndex ->
                            val keyToRead = if ((repetitionIndex + readIndex) % 100 == 0) {
                                bullshitKey
                            } else {
                                keyList.random()
                            }

                            val value = store.get(tx, keyToRead)
                            blackHole += value?.length ?: 1
                        }
                    }
                }.let(dataPoints::add)
            }
        }

        val timeAfterRead = System.currentTimeMillis()
        val totalTime = timeAfterRead - timeBeforeRead
        println("Black hole: ${blackHole}")
        println("Total Time: ${totalTime}ms")
        println(dataPoints.statistics().prettyPrint("Xodus read ${NUMBER_OF_READS} keys (${REPETITIONS} repetitions)"))
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