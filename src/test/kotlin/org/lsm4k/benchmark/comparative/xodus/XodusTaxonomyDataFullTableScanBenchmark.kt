package org.lsm4k.benchmark.comparative.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ByteIterator
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.StoreConfig
import org.lsm4k.model.command.Command
import org.lsm4k.model.command.KeyAndTSN
import java.io.File
import java.io.InputStream

object XodusTaxonomyDataFullTableScanBenchmark {

    @JvmStatic
    fun main(args: Array<String>) {
        println("STARTING BENCHMARK")
        val inputDir = File("/home/martin/Documents/lsm4k-test/taxonomyXodusBTree")

        var commandCount = 0
        var blackHole = 0L
        val timeBeforeRead = System.currentTimeMillis()
        Environments.newInstance(inputDir).use { environment ->
            environment.computeInReadonlyTransaction { tx ->
                val store = environment.openStore("data", StoreConfig.USE_EXISTING, tx)
                store.openCursor(tx).use { cursor ->
                    while (cursor.next) {
                        val binaryKey = cursor.key
                        val binaryValue = cursor.value
                        val keyAndTSN = XodusByteInputStream(binaryKey).use(KeyAndTSN.Companion::readFromStream)
                        val command = XodusByteInputStream(binaryValue).use(Command.Companion::readFromStream)
                        blackHole += command.value.size
                        blackHole += keyAndTSN.key.size
                        commandCount += 1
                    }
                }
            }
        }
        val timeAfterRead = System.currentTimeMillis()
        println("Read all ${commandCount} in Xodus directory '${inputDir.absolutePath}' in ${timeAfterRead - timeBeforeRead}ms.")
        println("Black hole: ${blackHole}")
    }

    private class XodusByteInputStream(
        private val iterator: ByteIterator
    ) : InputStream() {

        constructor(iterable: ByteIterable) : this(iterable.iterator())

        override fun read(): Int {
            if (!this.iterator.hasNext()) {
                return -1
            }
            // to convert negative byte values to positive
            // integers, we perform the "and 0xff". If we
            // don't do this, we get negative integers, which
            // is not what we want.
            return this.iterator.next().toInt() and 0xff
        }

    }

}