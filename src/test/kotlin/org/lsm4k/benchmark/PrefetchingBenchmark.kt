package org.lsm4k.benchmark

import org.lsm4k.api.DatabaseEngine
import org.lsm4k.api.LSM4KConfiguration
import org.lsm4k.util.IOExtensions.size
import org.lsm4k.util.UUIDExtensions.toBytes
import org.lsm4k.util.bytes.Bytes
import org.lsm4k.util.cron.CronUtils
import org.lsm4k.util.json.JsonUtil
import org.lsm4k.util.unit.BinarySize.Companion.Bytes
import org.lsm4k.util.unit.BinarySize.Companion.GiB
import org.lsm4k.util.unit.BinarySize.Companion.KiB
import org.lsm4k.util.unit.BinarySize.Companion.MiB
import java.nio.file.Files
import java.util.*
import java.util.concurrent.ThreadLocalRandom

object PrefetchingBenchmark {

    private const val NUMBER_OF_KEYS = 10_000

    private const val NUMBER_OF_COMMITS = 1_000

    private const val NUMBER_OF_CHANGES_PER_COMMIT = 200

    @JvmStatic
    fun main(args: Array<String>) {
        val tempDir = Files.createTempDirectory("lsm4kPrefetchingBenchmark").toFile()
        try {
            val config = LSM4KConfiguration(
                prefetchingThreads = 4,
                maxBlockSize = 8.MiB,
                blockCacheSize = 1.GiB,
                fileHeaderCacheSize = 200.MiB,
                checkpointOnShutdown = true,
                minorCompactionCron = CronUtils.cron("0 */1 * * * *"), // every minute
                checkpointCron = CronUtils.cron("0 */1 * * * *"), // every minute

            )

            DatabaseEngine.openOnDirectory(tempDir, config).use { engine ->
                println("Database Engine opened.")

                println("Generating ${NUMBER_OF_KEYS} unique random keys...")
                val keys = (0..<NUMBER_OF_KEYS).map { UUID.randomUUID().toBytes() }
                println("Generated ${NUMBER_OF_KEYS} unique random keys.")

                println("Inserting data...")

                engine.beginReadWriteTransaction().use { tx ->
                    tx.createNewStore("benchmark")
                    tx.commit()
                }

                repeat(NUMBER_OF_COMMITS) { commitIndex ->
                    engine.beginReadWriteTransaction().use { tx ->
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
                println(JsonUtil.writeJson(engine.statusReport(), prettyPrint = true))
                println()
                println("==========================================================")
                println()
                println()
                println("Closing Database Engine...")
            }
            println("Database Engine closed. Benchmark dir size: ${tempDir.size.Bytes.toHumanReadableString()}")
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