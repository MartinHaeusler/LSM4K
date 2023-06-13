package org.chronos.chronostore.benchmark

import org.chronos.chronostore.test.util.ChronoStoreMode
import org.chronos.chronostore.test.util.ChronoStoreTest
import org.chronos.chronostore.util.Bytes
import org.junit.jupiter.api.Test
import kotlin.concurrent.thread

class ReadWriteBenchmark {

    companion object {

        private const val NUMBER_OF_COMMITS = 10_000
        private const val WRITES_PER_COMMIT = 100
        private const val NUMBER_OF_UNIQUE_KEYS = 1000

        private const val NUMBER_OF_READER_THREADS = 4
        private const val NUMBER_OF_READS = 20_000
    }

    @Test
    fun canPerformParallelReadsAndWrites() {
        println("ATTACH PROFILER NOW!!")
        Thread.sleep(10_000)
        println("RUNNING")

        val uniqueKeys = (0 until NUMBER_OF_UNIQUE_KEYS).map { "key#${it}" }
        ChronoStoreMode.ONDISK.withChronoStore { chronoStore ->
            chronoStore.transaction { tx ->
                tx.createNewStore("test", versioned = true)
                tx.commit()
            }

            val writer = thread(name = "Writer") {
                println(Thread.currentThread().name + " :: started.")
                var value = 0
                val timeBefore = System.currentTimeMillis()
                repeat(NUMBER_OF_COMMITS) { c ->
                    chronoStore.transaction { tx ->
                        val store = tx.store("test")
                        repeat(WRITES_PER_COMMIT) { i ->
                            val keyBytes = Bytes(uniqueKeys.random())
                            if ((c + i % 7) == 0) {
                                store.delete(keyBytes)
                            } else {
                                val strValue = "${value}"
                                if(strValue.isEmpty()){
                                    throw IllegalStateException("String is empty!")
                                }
                                val valueBytes = Bytes(strValue)
                                store.put(keyBytes, valueBytes)
                            }
                            value += 1
                        }
                        tx.commit()
                    }
                    if(c % 100 == 0){
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
                        totalSum += chronoStore.transaction { tx ->
                            val store = tx.store("test")
                            val keyBytes = Bytes(uniqueKeys.random())
                            store.openCursorOnLatest().use { cursor ->
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
                                if(r % 100 == 0){
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
        }
    }

}