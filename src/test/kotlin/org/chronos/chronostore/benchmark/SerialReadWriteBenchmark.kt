package org.chronos.chronostore.benchmark

import mu.KotlinLogging
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.TransactionalStore.Companion.withCursor
import org.chronos.chronostore.test.util.ChronoStoreMode
import org.chronos.chronostore.util.bytes.BasicBytes
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import org.chronos.chronostore.util.unit.GiB
import org.chronos.chronostore.util.unit.MiB
import kotlin.random.Random
import kotlin.system.measureTimeMillis

object SerialReadWriteBenchmark {

    private val log = KotlinLogging.logger {}

    private const val NUMBER_OF_COMMITS = 10_000
    private const val WRITES_PER_COMMIT = 100
    private const val NUMBER_OF_UNIQUE_KEYS = 1000

    private const val NUMBER_OF_READS = 20_000

    private const val VALUE_SIZE_BYTES = 1024 * 10

    @JvmStatic
    fun main(args: Array<String>) {
        println("PRESS ENTER TO START")
        System.`in`.read()
        println("STARTING BENCHMARK")


        val random = Random(System.currentTimeMillis())
        val uniqueKeys = (0..<NUMBER_OF_UNIQUE_KEYS).map { "key#${it}" }

        val config = ChronoStoreConfiguration()
        config.maxForestSize = 1.GiB
        config.maxBlockSize = 32.MiB

        ChronoStoreMode.ONDISK.withChronoStore(config) { chronoStore ->
            chronoStore.transaction { tx ->
                tx.createNewStore("test")
                tx.commit()
            }

            val startTime = System.currentTimeMillis()

            log.info { "Starting writer." }
            measureTimeMillis {
                repeat(NUMBER_OF_COMMITS) { c ->
                    chronoStore.transaction { tx ->
                        val store = tx.getStore("test")
                        repeat(WRITES_PER_COMMIT) { i ->
                            val keyBytes = BasicBytes(uniqueKeys.random())
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
                        val runtime = System.currentTimeMillis() - startTime
                        val stallTime = ChronoStoreStatistics.TOTAL_WRITE_STALL_TIME_MILLIS.get()
                        val stallTimePercent = (stallTime / runtime.toDouble()) * 100
                        log.info { "Commit #${c} successful. Stall time: ${stallTime}ms (${stallTimePercent}%)." }
                    }
                }
            }.let { log.info { "Writer completed in ${it}ms." } }

            log.info { "Flushing changes to disk" }
            measureTimeMillis {
                chronoStore.mergeService.flushAllInMemoryStoresToDisk()
            }.let { log.info { "Flushed changes to disk in ${it}ms." } }

            log.info { "Performing major compaction" }
            measureTimeMillis {
                chronoStore.mergeService.performMajorCompaction()
            }.let { log.info { "Major compaction took ${it}ms." } }

            log.info { "root path: ${chronoStore.rootPath}" }

            var totalSize = 0L
            measureTimeMillis {
                repeat(NUMBER_OF_READS) { r ->
                    totalSize += chronoStore.transaction { tx ->
                        val store = tx.getStore("test")
                        val keyBytes = BasicBytes(uniqueKeys.random())
                        store.withCursor { cursor ->
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
                            if (r % 10 == 0 && r > 0) {
                                log.info { "Read #${r} successful." }
                            }
                            sum
                        }
                    }
                }
            }.let { log.info { "Read complete. Time: ${it}ms, Total data read: ${Bytes.formatSize(totalSize)}" } }

            log.info { "FINAL STATISTICS" }
            println(ChronoStoreStatistics.snapshot().prettyPrint())
        }
    }

}