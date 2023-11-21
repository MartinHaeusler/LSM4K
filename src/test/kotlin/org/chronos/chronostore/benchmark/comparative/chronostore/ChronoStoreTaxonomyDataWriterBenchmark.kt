package org.chronos.chronostore.benchmark.comparative.chronostore

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.IOExtensions.size
import org.chronos.chronostore.util.bytes.Bytes
import org.xerial.snappy.Snappy
import java.io.File

object ChronoStoreTaxonomyDataWriterBenchmark {

    val inputFile = File("/home/martin/Documents/chronostore-test/rawCommandsBinary")
    val storeDir = File("/home/martin/Documents/chronostore-test/taxonomyChronoStore")

    @JvmStatic
    fun main(args: Array<String>) {
        // access snappy to get native initialization out of the way.
        // This only happens once per JVM restart and we don't want to include it in the benchmark.
        Snappy.getNativeLibraryVersion()

        println("Attach profiler now! Press any key to continue")
        System.`in`.read()

        println("STARTING BENCHMARK")

        if(storeDir.exists()){
            storeDir.deleteRecursively()
        }
        storeDir.mkdirs()

        ChronoStore.openOnDirectory(this.storeDir, ChronoStoreConfiguration()).use { chronoStore ->
            inputFile.inputStream().buffered().use { input ->
                val commandSequence = generateSequence {
                    Command.readFromStreamOrNull(input)
                }

                val timeBefore = System.currentTimeMillis()
                chronoStore.transaction { tx ->
                    tx.createNewStore("data", false)
                    tx.commit()
                }

                commandSequence.chunked(10_000).forEach { chunk ->
                    chronoStore.transaction { tx ->
                        val store = tx.getStore("data")
                        for (entry in chunk) {
                            store.put(entry.key, entry.value)
                        }
                        tx.commit()
                    }
                }
                val timeAfter = System.currentTimeMillis()
                println("Wrote entries into ${storeDir.path} with ${Bytes.formatSize(storeDir.size)} in ${timeAfter - timeBefore}ms.")
            }
        }
    }

}