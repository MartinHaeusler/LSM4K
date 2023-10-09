package org.chronos.chronostore.benchmark.comparative.xodus

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.StoreConfig
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.IOExtensions.size
import org.chronos.chronostore.util.bytes.Bytes
import org.chronos.chronostore.util.unit.Bytes
import java.io.File
import java.nio.file.Files

object XodusTaxonomyDataWriterBenchmark {

    @JvmStatic
    fun main(args: Array<String>) {
        val inputFile = File("/home/martin/Documents/chronostore-test/rawCommandsBinary")
        println("Streaming in data from '${inputFile.absolutePath}' (${inputFile.length().Bytes.toHumanReadableString()})")

        inputFile.inputStream().buffered().use { input ->
            val commandSequence = generateSequence {
                Command.readFromStreamOrNull(input)
            }

            val tempDir = Files.createTempDirectory("benchmarkXodusTaxonomyWriter").toFile()

            println("Writing to directory: ${tempDir}")

            val environment = Environments.newInstance(tempDir)

            var commandCount = 0

            val timeBefore = System.currentTimeMillis()
            environment.computeInTransaction { tx ->
                environment.openStore("data", StoreConfig.WITHOUT_DUPLICATES, tx)
                tx.commit()
            }


            commandSequence.chunked(10_000).forEach { chunk ->
                environment.computeInTransaction { tx ->
                    val store = environment.openStore("data", StoreConfig.USE_EXISTING, tx)
                    for (command in chunk) {
                        val xodusKey = ArrayByteIterable(command.keyAndTimestamp.toBytes().toSharedArray())
                        val xodusValue = ArrayByteIterable(command.toBytes().toSharedArray())
                        store.put(tx, xodusKey, xodusValue)
                        commandCount++
                    }
                    tx.commit()
                }
                println("Wrote ${commandCount} commands.")
            }


            val timeAfter = System.currentTimeMillis()

            println("Wrote ${commandCount} entries into ${tempDir.path} with ${Bytes.formatSize(tempDir.size)} in ${timeAfter - timeBefore}ms.")
        }
    }


}