package org.chronos.chronostore.benchmark

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.util.IOExtensions.size
import org.chronos.chronostore.util.UUIDExtensions.toBytes
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.cron.CronUtils
import org.chronos.chronostore.util.json.JsonUtil
import org.chronos.chronostore.util.unit.BinarySize.Companion.Bytes
import org.chronos.chronostore.util.unit.BinarySize.Companion.GiB
import org.chronos.chronostore.util.unit.BinarySize.Companion.KiB
import org.chronos.chronostore.util.unit.BinarySize.Companion.MiB
import java.nio.file.Files
import java.util.*
import java.util.concurrent.ThreadLocalRandom

object PrefetchingBenchmark {

    private const val NUMBER_OF_KEYS = 10_000

    private const val NUMBER_OF_COMMITS = 1_000

    private const val NUMBER_OF_CHANGES_PER_COMMIT = 200


    // FIXME:
    //
    // Good:
    // + we survive the benchmark, as in: the store doesn't crash. Yay!
    // + WAL shortening seems to work fine, including the corner cases introduced by partial commits
    // + manifest seems to be working just fine
    // + no concurrency-related issues detected
    // + prefetcher claims to work 100% of the time and we had zero I/O wait time in the cursors (need to investigate -> that's a little too good to be true)
    //
    // Bad:
    // - We have seemingly random slowdown "hiccups" in the write process. Stalling going on? Maybe the memory manager needs to start flushing data sooner?
    // - When there's a compaction going on while the store is shutting down, we fail with a PANIC and:
    //   "An unexpected error occurred during Minor Compaction of store 'benchmark': java.nio.channels.ClosedByInterruptException', cause: java.nio.channels.ClosedByInterruptException".
    //   Try to make this more graceful.

    @JvmStatic
    fun main(args: Array<String>) {
        val tempDir = Files.createTempDirectory("chronostorePrefetchingBenchmark").toFile()
        try {
            val config = ChronoStoreConfiguration(
                prefetchingThreads = 4,
                maxBlockSize = 8.MiB,
                blockCacheSize = 1.GiB,
                fileHeaderCacheSize = 200.MiB,
                checkpointOnShutdown = true,
                minorCompactionCron = CronUtils.cron("0 */1 * * * *"), // every minute
                checkpointCron = CronUtils.cron("0 */1 * * * *"), // every minute

            )

            ChronoStore.openOnDirectory(tempDir, config).use { chronoStore ->
                println("ChronoStore opened.")

                println("Generating ${NUMBER_OF_KEYS} unique random keys...")
                val keys = (0..<NUMBER_OF_KEYS).map { UUID.randomUUID().toBytes() }
                println("Generated ${NUMBER_OF_KEYS} unique random keys.")

                println("Inserting data...")

                chronoStore.beginTransaction().use { tx ->
                    tx.createNewStore("benchmark")
                    tx.commit()
                }

                repeat(NUMBER_OF_COMMITS) { commitIndex ->
                    chronoStore.beginTransaction().use { tx ->
                        println("Transaction #${commitIndex + 1} started.")

                        val store = tx.getStore("benchmark")

                        repeat(NUMBER_OF_CHANGES_PER_COMMIT) {
                            val key = keys.random()
                            val data = createRandomData()
                            store.put(key, data)
                        }

                        tx.commit()
                        println("Transaction #${commitIndex + 1} committed.")
                    }
                }

                println()
                println()
                println("==========================================================")
                println("STATUS REPORT")
                println("----------------------------------------------------------")
                println()
                println(JsonUtil.writeJson(chronoStore.statusReport(), prettyPrint = true))
                println()
                println("==========================================================")
                println()
                println()
                println("Closing ChronoStore...")
            }
            println("ChronoStore closed. Benchmark dir size: ${tempDir.size.Bytes.toHumanReadableString()}")
            println()
        } finally {
//            println("Erasing benchmark files in '${tempDir.absolutePath}'...")
//            tempDir.deleteRecursively()
//            println("Benchmark files erased.")
        }
    }

    private fun createRandomData(): Bytes {
        val array = ByteArray(10.KiB.bytes.toInt())
        ThreadLocalRandom.current().nextBytes(array)
        return Bytes.wrap(array)
    }

}