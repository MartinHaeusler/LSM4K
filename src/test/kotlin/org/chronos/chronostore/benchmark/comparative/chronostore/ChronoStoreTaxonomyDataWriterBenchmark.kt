package org.chronos.chronostore.benchmark.comparative.chronostore

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.impl.ChronoStoreImpl
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.IOExtensions.size
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.unit.BinarySize.Companion.MiB
import org.xerial.snappy.Snappy
import java.io.File

object ChronoStoreTaxonomyDataWriterBenchmark {

    private val inputFile = File("/home/martin/Documents/chronostore-test/rawCommandsBinary")
    private val storeDir = File("/home/martin/Documents/chronostore-test/taxonomyChronoStore")

    @JvmStatic
    fun main(args: Array<String>) {
        // access snappy to get native initialization out of the way.
        // This only happens once per JVM restart and we don't want to include it in the benchmark.
        Snappy.getNativeLibraryVersion()

        println("STARTING BENCHMARK")

        if (storeDir.exists()) {
            storeDir.deleteRecursively()
        }
        storeDir.mkdirs()

        val configuration = ChronoStoreConfiguration(
            maxBlockSize = 16.MiB,
            // disable the checkpoints during shutdown on purpose to provoke potential errors in the reader.
            checkpointOnShutdown = false,
        )

        val statisticsReport = ChronoStore.openOnDirectory(this.storeDir, configuration).use { chronoStore ->
            chronoStore.statistics.startCollection()
            inputFile.inputStream().buffered().use { input ->
                val commandSequence = generateSequence {
                    Command.readFromStreamOrNull(input)
                }

                val timeBefore = System.currentTimeMillis()
                chronoStore.readWriteTransaction { tx ->
                    tx.createNewStore("data")
                    tx.commit()
                }

                var entries = 0

                var transactionCount = 0
                commandSequence.chunked(10_000).forEach { chunk ->
                    chronoStore.readWriteTransaction { tx ->
                        val store = tx.getStore("data")
                        for (entry in chunk) {
                            store.put(entry.keyAndTSN.toBytes(), entry.value)
                            entries++
                        }
                        transactionCount++
                        tx.commit()
                    }
                }
                val timeAfter = System.currentTimeMillis()
                println("Wrote ${entries} entries into ${storeDir.path} with ${Bytes.formatSize(storeDir.size)} in ${timeAfter - timeBefore}ms. ${transactionCount} transactions were committed.")

                chronoStore as ChronoStoreImpl

//                println("Flushing all data")
//                chronoStore.mergeService.flushAllInMemoryStoresToDisk()
//                println("Compacting data")
//                chronoStore.mergeService.performMajorCompaction()
                println("Done")
            }

            chronoStore.statistics.report()!!
        }

        println()
        println()
        println()

        println(statisticsReport.prettyPrint())
    }

}