package org.chronos.chronostore.benchmark

import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.TransactionBoundStore.Companion.withCursorOnLatest
import org.chronos.chronostore.test.util.ChronoStoreMode
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.unit.GiB
import org.chronos.chronostore.util.unit.KiB
import kotlin.random.Random

/**
 * Aim of this benchmark: find out how much read performance suffers when there are multiple chronostore files.
 */
object MultiFileReadRegressionBenchmark {

    @JvmStatic
    fun main(args: Array<String>) {
        val random = Random(System.currentTimeMillis())

        val config = ChronoStoreConfiguration()
        config.maxForestSize = 1.GiB
        // don't merge automatically
        config.mergeInterval = null
        // disable caching
        config.blockCacheSize = null

        ChronoStoreMode.ONDISK.withChronoStore(config) { chronoStore ->
            chronoStore.transaction { tx ->
                tx.createNewStore("test", versioned = true)
                tx.commit()
            }

            repeat(10) {
                chronoStore.transaction { tx ->
                    val store = tx.getStore("test")
                    repeat(10_000) { index ->
                        store.put(Bytes.stableInt(index), Bytes.random(random, 10.KiB.bytes.toInt()))
                    }
                    tx.commit()
                }

                chronoStore.mergeService.flushAllInMemoryStoresToDisk()
            }

            var entriesInHead = 0L
            var blackHole = 0L

            chronoStore.mergeService.performMajorCompaction()

            val numberOfFiles = chronoStore.transaction { tx ->
                val store = tx.getStore("test")
                val list = store.store.directory.list()
                list.forEach { println(it) }
                list.size
            }

            println("Preparations complete. Files in store: ${numberOfFiles}")

            val dataPoints = mutableListOf<Long>()
            repeat(30){ runIndex ->
                entriesInHead = 0
                val timeBefore = System.currentTimeMillis()
                chronoStore.transaction { tx ->
                    val store = tx.getStore("test")
                    store.withCursorOnLatest { cursor ->
                        cursor.firstOrThrow()
                        for ((key, value) in cursor.ascendingEntrySequenceFromHere()) {
                            entriesInHead += 1
                            blackHole += key.size
                            blackHole += value.size
                        }
                    }
                }
                val timeAfter = System.currentTimeMillis()
                val duration = timeAfter - timeBefore
                dataPoints += duration
                println("Run #${runIndex}: ${duration}ms")
            }

            println("Iteration over keyspace with ${entriesInHead} keys over ${numberOfFiles} files took ${dataPoints.average().toLong()}ms.")
            println("Black Hole: ${blackHole}")
        }


    }

}