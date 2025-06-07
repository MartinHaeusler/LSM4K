package io.github.martinhaeusler.lsm4k.benchmark.comparative.lsm4k

import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.model.command.KeyAndTSN
import java.io.File

object RawCommandsBinaryDebug {

    @JvmStatic
    fun main(args: Array<String>) {
        val inputFile = File("/home/martin/Documents/lsm4k-test/rawCommandsBinary")
        var index = 0
        inputFile.inputStream().use { inputStream ->
            while (true) {
                if (index % 100 == 0) {
                    println("checking entry ${index}...")
                }

                val command = Command.readFromStreamOrNull(inputStream)
                    ?: break

                val isKeyAndTSN = KeyAndTSN.readFromBytesOrNull(command.key) != null
                if (isKeyAndTSN) {
                    println("Command #${index.toString().padStart(12, '0')} has a key-and-timestamp in the key!")
                }

                index++
            }
        }
        println("DONE after ${index} entries.")
    }


}