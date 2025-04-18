package org.chronos.chronostore.benchmark

import org.chronos.chronostore.benchmark.util.Statistics
import org.chronos.chronostore.benchmark.util.Statistics.Companion.statistics
import org.chronos.chronostore.io.fileaccess.FileChannelDriver
import org.chronos.chronostore.io.fileaccess.MemorySegmentFileDriver
import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriverFactory
import org.chronos.chronostore.io.vfs.VirtualFile
import org.chronos.chronostore.test.util.VFSMode
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