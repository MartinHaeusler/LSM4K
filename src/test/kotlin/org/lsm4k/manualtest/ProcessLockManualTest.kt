package org.lsm4k.manualtest

import com.google.common.util.concurrent.Uninterruptibles
import org.lsm4k.api.DatabaseEngine
import java.io.File
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

object ProcessLockManualTest {

    @JvmStatic
    fun main(args: Array<String>) {
        DatabaseEngine.openOnDirectory(File("/home/martin/Desktop/lsm4ktest")).use { engine ->
            println("DatabaseEngine opened on '${engine.rootPath}'.")
            println("Run another instance of this process and check that it fails to open the database because of the process lock.")
            println("This process will now sleep for 30 seconds and then close this store again.")

            repeat(30) {
                println(30 - it)
                Uninterruptibles.sleepUninterruptibly(1.seconds.toJavaDuration())
            }

        }
        println("DatabaseEngine closed.")
    }

}