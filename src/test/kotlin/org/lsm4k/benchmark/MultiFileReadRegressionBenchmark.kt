package org.lsm4k.benchmark

import org.lsm4k.api.LSM4KConfiguration
import org.lsm4k.api.TransactionalStore.Companion.withCursor
import org.lsm4k.impl.transaction.TransactionalStoreInternal
import org.lsm4k.lsm.LSMTreeFile
import org.lsm4k.test.util.LSM4KMode
import org.lsm4k.util.bytes.Bytes
import org.lsm4k.util.unit.BinarySize.Companion.GiB
import org.lsm4k.util.unit.BinarySize.Companion.KiB
import org.lsm4k.util.unit.BinarySize.Companion.MiB
import kotlin.random.Random

/**
 * Aim of this benchmark: find out how much read performance suffers when there are multiple LSM files.
 */
object MultiFileReadRegressionBenchmark {

    @JvmStatic
    fun main(args: Array<String>) {
        val random = Random(System.currentTimeMillis())

        val config = LSM4KConfiguration(
            maxForestSize = 1.GiB,
            // don't merge automatically
            minorCompactionCron = null,
            majorCompactionCron = null,
            // use a very small cache
            blockCacheSize = 20.MiB,
        )

        LSM4KMode.ONDISK.withDatabaseEngine(config) { engine ->
            engine.readWriteTransaction { tx ->
                tx.createNewStore("test")
                tx.commit()
            }

            repeat(10) {
                engine.readWriteTransaction { tx ->
                    val store = tx.getStore("test")
                    repeat(10_000) { index ->
                        store.put(Bytes.stableInt(index), Bytes.random(random, 10.KiB.bytes.toInt()))
                    }
                    tx.commit()
                }

                engine.flushAllStoresSynchronous()
            }

            var entriesInHead = 0L
            var blackHole = 0L

//            engine.mergeService.performMajorCompaction()

            // delete the files which are now unused
            engine.garbageCollectionSynchronous()

            val numberOfFiles = engine.readWriteTransaction { tx ->
                val store = tx.getStore("test")
                val list = (store as TransactionalStoreInternal).store.directory.list().filter { it.endsWith(LSMTreeFile.FILE_EXTENSION) }
                list.forEach { println(it) }
                list.size
            }

            println("Preparations complete. Files in store: ${numberOfFiles}")

            val dataPoints = mutableListOf<Long>()
            repeat(30) { runIndex ->
                entriesInHead = 0
                val timeBefore = System.currentTimeMillis()
                engine.readWriteTransaction { tx ->
                    val store = tx.getStore("test")
                    store.withCursor { cursor ->
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