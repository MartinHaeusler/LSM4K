package io.github.martinhaeusler.lsm4k.benchmark.comparative.lsm4k

import io.github.martinhaeusler.lsm4k.api.DatabaseEngine
import io.github.martinhaeusler.lsm4k.api.LSM4KConfiguration
import io.github.martinhaeusler.lsm4k.impl.DatabaseEngineImpl
import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.util.IOExtensions.size
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.MiB
import java.io.File

object TaxonomyDataWriterBenchmark {

    private val inputFile = File("/home/martin/Documents/lsm4k-test/rawCommandsBinary")
    private val storeDir = File("/home/martin/Documents/lsm4k-test/taxonomyLSM4K")

    @JvmStatic
    fun main(args: Array<String>) {

        println("STARTING BENCHMARK")

        if (storeDir.exists()) {
            storeDir.deleteRecursively()
        }
        storeDir.mkdirs()

        val configuration = LSM4KConfiguration(
            maxBlockSize = 16.MiB,
            // disable the checkpoints during shutdown on purpose to provoke potential errors in the reader.
            checkpointOnShutdown = false,
        )

        val statisticsReport = DatabaseEngine.openOnDirectory(this.storeDir, configuration).use { engine ->
            engine.statistics.startCollection()
            inputFile.inputStream().buffered().use { input ->
                val commandSequence = generateSequence {
                    Command.readFromStreamOrNull(input)
                }

                val timeBefore = System.currentTimeMillis()
                engine.readWriteTransaction { tx ->
                    tx.createNewStore("data")
                    tx.commit()
                }

                var entries = 0

                var transactionCount = 0
                commandSequence.chunked(10_000).forEach { chunk ->
                    engine.readWriteTransaction { tx ->
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

                engine as DatabaseEngineImpl

//                println("Flushing all data")
//                engine.mergeService.flushAllInMemoryStoresToDisk()
//                println("Compacting data")
//                engine.mergeService.performMajorCompaction()
                println("Done")
            }

            engine.statistics.report()!!
        }

        println()
        println()
        println()

        println(statisticsReport.prettyPrint())
    }

}