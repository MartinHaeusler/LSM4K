package org.chronos.chronostore.benchmark

import org.chronos.chronostore.test.util.ChronoStoreMode
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import kotlin.random.Random
import kotlin.system.measureTimeMillis

object SerialReadWriteBenchmark {


    private const val NUMBER_OF_COMMITS = 10_000
    private const val WRITES_PER_COMMIT = 100
    private const val NUMBER_OF_UNIQUE_KEYS = 1000

    private const val NUMBER_OF_READS = 20_000

    private const val VALUE_SIZE_BYTES = 1024 * 10

    @JvmStatic
    fun main(args: Array<String>) {
        println("ATTACH PROFILER NOW!!")
        Thread.sleep(10_000)
        println("RUNNING")

        val random = Random(System.currentTimeMillis())
        val uniqueKeys = (0 until NUMBER_OF_UNIQUE_KEYS).map { "key#${it}" }

        ChronoStoreMode.ONDISK.withChronoStore { chronoStore ->
            chronoStore.transaction { tx ->
                tx.createNewStore("test", versioned = true)
                tx.commit()
            }

            println("Starting writer.")
            measureTimeMillis {
                repeat(NUMBER_OF_COMMITS) { c ->
                    chronoStore.transaction { tx ->
                        val store = tx.store("test")
                        repeat(WRITES_PER_COMMIT) { i ->
                            val keyBytes = Bytes(uniqueKeys.random())
                            if ((c + i % 7) == 0) {
                                store.delete(keyBytes)
                            } else {
                                val valueBytes = Bytes.random(random, VALUE_SIZE_BYTES)
                                store.put(keyBytes, valueBytes)
                            }
                        }
                        tx.commit()
                    }
                    if (c % 100 == 0) {
                        println("${Thread.currentThread().name} :: Commit #${c} successful.")
                    }
                }
            }.let { println("Writer completed in ${it}ms.") }

            println("Flushing changes to disk")
            measureTimeMillis {
                chronoStore.mergeService.flushAllInMemoryStoresToDisk()
            }.let { println("Flushed changes to disk in ${it}ms.") }

            println("Performing major compaction")
            measureTimeMillis {
                chronoStore.mergeService.mergeNow(true)
            }.let { println("Major compaction took ${it}ms.") }

            println("root path: ${chronoStore.rootPath}")

            var totalSize = 0L
            measureTimeMillis {
                repeat(NUMBER_OF_READS) { r ->
                    totalSize += chronoStore.transaction { tx ->
                        val store = tx.store("test")
                        val keyBytes = Bytes(uniqueKeys.random())
                        store.openCursorOnLatest().use { cursor ->
                            val sum = if (cursor.seekExactlyOrNext(keyBytes)) {
                                var i = 0
                                var sum = 0
                                do {
                                    sum += cursor.value.size
                                    i++
                                } while (i < 100 && cursor.next())
                                sum
                            } else {
                                0
                            }
                            if (r % 1000 == 0 && r > 0) {
                                println("${Thread.currentThread().name} :: Read #${r} successful.")
                            }
                            sum
                        }
                    }
                }
            }.let { println("Read complete. Time: ${it}ms, Total data read: ${Bytes.formatSize(totalSize)}") }

            println("FINAL STATISTICS")
            println(ChronoStoreStatistics.snapshot().prettyPrint())
        }
    }

}