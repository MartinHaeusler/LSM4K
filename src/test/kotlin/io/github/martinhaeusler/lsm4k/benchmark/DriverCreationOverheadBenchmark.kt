package io.github.martinhaeusler.lsm4k.benchmark

import io.github.martinhaeusler.lsm4k.benchmark.util.Statistics
import io.github.martinhaeusler.lsm4k.benchmark.util.Statistics.Companion.statistics
import io.github.martinhaeusler.lsm4k.io.fileaccess.FileChannelDriver
import io.github.martinhaeusler.lsm4k.io.fileaccess.MemorySegmentFileDriver
import io.github.martinhaeusler.lsm4k.io.fileaccess.RandomFileAccessDriverFactory
import io.github.martinhaeusler.lsm4k.io.vfs.VirtualFile
import io.github.martinhaeusler.lsm4k.test.util.VFSMode
import kotlin.system.measureNanoTime

object DriverCreationOverheadBenchmark {

    @JvmStatic
    fun main(args: Array<String>) {
        VFSMode.ONDISK.withVFS { vfs ->
            val file = vfs.file("test")
            file.create()

            val fileChannelStats = benchmarkDriverOpening(
                driverFactory = FileChannelDriver.Factory,
                file = file,
                runs = 1000,
            )

            println()
            println()
            println(fileChannelStats.prettyPrint("FileChannelDriver"))
            println()

            val memorySegmentStats = benchmarkDriverOpening(
                driverFactory = MemorySegmentFileDriver.Factory,
                file = file,
                runs = 1000,
            )

            println(memorySegmentStats.prettyPrint("MemorySegmentFileDriver"))
            println()
        }
    }

    private fun benchmarkDriverOpening(driverFactory: RandomFileAccessDriverFactory, file: VirtualFile, runs: Int): Statistics {
        val dataPoints = mutableListOf<Long>()
        repeat(runs) {
            val nanos = measureNanoTime {
                repeat(100) {
                    driverFactory.createDriver(file).close()
                }
            }
            dataPoints += nanos
        }
        return dataPoints.asSequence().map { it.toDouble() / 1000000 }.statistics()
    }

}