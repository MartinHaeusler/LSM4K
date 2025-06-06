package org.lsm4k.benchmark

import io.github.oshai.kotlinlogging.KotlinLogging
import org.lsm4k.api.LSM4KConfiguration
import org.lsm4k.api.TransactionalStore.Companion.withCursor
import org.lsm4k.test.util.LSM4KMode
import org.lsm4k.util.bytes.BasicBytes
import org.lsm4k.util.bytes.Bytes
import org.lsm4k.util.unit.BinarySize.Companion.GiB
import org.lsm4k.util.unit.BinarySize.Companion.MiB
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

        val config = LSM4KConfiguration(
            maxForestSize = 1.GiB,
            maxBlockSize = 32.MiB,
        )

        LSM4KMode.ONDISK.withDatabaseEngine(config) { engine ->
            engine.statistics.startCollection()
            engine.readWriteTransaction { tx ->
                tx.createNewStore("test")
                tx.commit()
            }

            val startTime = System.currentTimeMillis()

            log.info { "Starting writer." }
            measureTimeMillis {
                repeat(NUMBER_OF_COMMITS) { c ->
                    engine.readWriteTransaction { tx ->
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
                        val stallTime = engine.statistics.report()!!.totalWriteStallTimeMillis
                        val stallTimePercent = (stallTime / runtime.toDouble()) * 100
                        log.info { "Commit #${c} successful. Stall time: ${stallTime}ms (${stallTimePercent}%)." }
                    }
                }
            }.let { log.info { "Writer completed in ${it}ms." } }

            log.info { "Flushing changes to disk" }
            measureTimeMillis {
                engine.flushAllStoresSynchronous()
            }.let { log.info { "Flushed changes to disk in ${it}ms." } }

//            log.info { "Performing major compaction" }
//            measureTimeMillis {
//                engine.performMajorCompaction()
//            }.let { log.info { "Major compaction took ${it}ms." } }

            log.info { "root path: ${engine.rootPath}" }

            var totalSize = 0L
            measureTimeMillis {
                repeat(NUMBER_OF_READS) { r ->
                    totalSize += engine.readWriteTransaction { tx ->
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
            println(engine.statistics.report()!!.prettyPrint())
        }
    }

}