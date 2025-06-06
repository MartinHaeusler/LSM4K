package org.lsm4k.benchmark

import org.lsm4k.api.TransactionalStore.Companion.withCursor
import org.lsm4k.test.util.LSM4KMode
import org.lsm4k.util.bytes.BasicBytes
import kotlin.concurrent.thread

object ParallelReadWriteBenchmark {


    private const val NUMBER_OF_COMMITS = 10_000
    private const val WRITES_PER_COMMIT = 100
    private const val NUMBER_OF_UNIQUE_KEYS = 1000

    private const val NUMBER_OF_READER_THREADS = 4
    private const val NUMBER_OF_READS = 20_000

    @JvmStatic
    fun main(args: Array<String>) {
        println("ATTACH PROFILER NOW!!")
        Thread.sleep(10_000)
        println("RUNNING")

        val uniqueKeys = (0 until NUMBER_OF_UNIQUE_KEYS).map { "key#${it}" }
        LSM4KMode.ONDISK.withDatabaseEngine { engine ->
            engine.statistics.startCollection()

            engine.readWriteTransaction { tx ->
                tx.createNewStore("test")
                tx.commit()
            }

            val writer = thread(name = "Writer") {
                println(Thread.currentThread().name + " :: started.")
                var value = 0
                val timeBefore = System.currentTimeMillis()
                repeat(NUMBER_OF_COMMITS) { c ->
                    engine.readWriteTransaction { tx ->
                        val store = tx.getStore("test")
                        repeat(WRITES_PER_COMMIT) { i ->
                            val keyBytes = BasicBytes(uniqueKeys.random())
                            if ((c + i % 7) == 0) {
                                store.delete(keyBytes)
                            } else {
                                val strValue = "${value}"
                                if (strValue.isEmpty()) {
                                    throw IllegalStateException("String is empty!")
                                }
                                val valueBytes = BasicBytes(strValue)
                                store.put(keyBytes, valueBytes)
                            }
                            value += 1
                        }
                        tx.commit()
                    }
                    if (c % 100 == 0) {
                        println("${Thread.currentThread().name} :: Commit #${c} successful.")
                    }
                }
                val timeAfter = System.currentTimeMillis()
                println("Writer completed in ${timeAfter - timeBefore}ms.")
            }
            val readers = (0 until NUMBER_OF_READER_THREADS).map { readerNumber ->
                thread(name = "Reader#${readerNumber}") {
                    println(Thread.currentThread().name + " :: started.")
                    val timeBefore = System.currentTimeMillis()
                    var totalSum = 0
                    repeat(NUMBER_OF_READS) { r ->
                        totalSum += engine.readWriteTransaction { tx ->
                            val store = tx.getStore("test")
                            val keyBytes = BasicBytes(uniqueKeys.random())
                            store.withCursor { cursor ->
                                val sum = if (cursor.seekExactlyOrNext(keyBytes)) {
                                    var i = 0
                                    var sum = 0
                                    do {
                                        sum += cursor.value.asString().toInt()
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
                    val timeAfter = System.currentTimeMillis()
                    println(Thread.currentThread().name + " :: Result: ${totalSum}, Time: ${timeAfter - timeBefore}ms")
                }
            }

            writer.join()
            for (reader in readers) {
                reader.join()
            }

            println("FINAL STATISTICS")
            println(engine.statistics.report()!!.prettyPrint())
        }
    }

}