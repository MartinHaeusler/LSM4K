package io.github.martinhaeusler.lsm4k.benchmark.comparative.xodus

import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.util.IOExtensions.size
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.unit.BinarySize.Companion.Bytes
import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.StoreConfig
import java.io.File
import java.nio.file.Files

object XodusTaxonomyDataWriterBenchmark {

    @JvmStatic
    fun main(args: Array<String>) {
        val inputFile = File("/home/martin/Documents/lsm4k-test/rawCommandsBinary")
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
                        val xodusKey = ArrayByteIterable(command.keyAndTSN.toBytes().toSharedArrayUnsafe())
                        val xodusValue = ArrayByteIterable(command.toBytes().toSharedArrayUnsafe())
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