package org.chronos.chronostore.lsm

import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronostore.async.executor.AsyncTaskManager
import org.chronos.chronostore.async.executor.TaskExecutionResult
import org.chronos.chronostore.async.taskmonitor.TaskMonitor
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.forEach
import org.chronos.chronostore.async.taskmonitor.TaskMonitor.Companion.mainTask
import org.chronos.chronostore.lsm.merge.tasks.FlushInMemoryTreeToDiskTask
import org.chronos.chronostore.manifest.ManifestFile
import org.chronos.chronostore.util.logging.LogExtensions.perfTrace
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics
import org.chronos.chronostore.util.unit.BinarySize.Companion.Bytes
import java.util.concurrent.Future
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.write
import kotlin.math.min

/**
 * Manager for a "forest" of [LSMTree]s for the purpose of memory management.
 *
 * When a lot of data is being written to the store, the flush task may not keep up with the writer.
 * This causes the RAM footprint of the store to grow over time until we eventually hit the JVM heap
 * size limit and crash. To prevent that from happening, a limit needs to be introduced.
 *
 * This manager keeps track of the data held in memory for each tree (called "actualTreeSize", measured in bytes).
 * The total across all trees is called "actualForestSize" (measured in bytes). The manager will defer (stall)
 * any writes if "actualForestSize" exceeds the configured maximum. Writes will be unstalled if and only if the
 * actual forest size decreases. This manager is responsible for dispatching the flush tasks. However, it does NOT
 * keep track of the tasks themselves. Instead, we manage a "virtualTreeSize" per tree (measured in bytes) which
 * is always less than or equal to "actualTreeSize". This virtual size gets reduced whenever a flush task is
 * scheduled and represents the expected size of the tree AFTER the flush (usually zero). When new data comes in,
 * both the virtual size and the actual size increase. When a flush completes, the actual size gets reduced by
 * the amount of flushed data; the virtual size stays the same. The manager will schedule a flush task every time
 * the virtual (!!) forest size crosses a configured threshold (e.g. 60% of capacity filled). This task will target
 * the tree with the largest virtual size. If one flush is not enough to bring the virtual forest size below the
 * threshold, further flushes on the trees with the largest virtual sizes will be launched until the virtual forest
 * size becomes less than the configured threshold.
 */
class LSMForestMemoryManager(
    private val asyncTaskManager: AsyncTaskManager,
    private val maxForestSize: Long,
    private val flushThresholdSize: Long,
    private val manifestFile: ManifestFile,
) {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    private val treeStats = mutableMapOf<LSMTree, TreeStats>()

    private val lock = ReentrantReadWriteLock(true)

    private val actualForestSizeReducedCondition = lock.writeLock().newCondition()

    @Volatile
    var actualForestSize: Long = 0L
        private set

    @Volatile
    var virtualForestSize: Long = 0L
        private set

    init {
        require(maxForestSize > 0) {
            "Argument 'maxForestSize' (${maxForestSize}) must be positive!"
        }
        require(flushThresholdSize <= maxForestSize) {
            "Argument 'flushThresholdSize' (${flushThresholdSize}) must not be larger than argument 'maxForestSize' (${maxForestSize})!"
        }

        log.info { "Forest Size: ${maxForestSize.Bytes.toHumanReadableString()}, Flush Threshold: ${flushThresholdSize.Bytes.toHumanReadableString()}" }
    }

    fun addTree(tree: LSMTree) {
        this.lock.write {
            treeStats.computeIfAbsent(tree, ::TreeStats)
            val virtualTreeSize = treeStats.getValue(tree).virtualTreeSize
            val actualTreeStats = treeStats.getValue(tree).actualTreeSize
            this.actualForestSize += actualTreeStats
            this.virtualForestSize += virtualTreeSize
        }
    }

    fun onBeforeInMemoryInsert(tree: LSMTree, bytesToInsert: Long) {
        this.lock.write {
            var stallBegin = -1L
            while (this.actualForestSize >= maxForestSize) {
                if (stallBegin < 0) {
                    stallBegin = System.currentTimeMillis()
                }
                log.perfTrace {
                    "Stalling write to '${tree.path}' because the in-memory buffer is full." +
                        " Write will continue after the flush task has been completed."
                }
                // We have too much data that is waiting to be flushed.
                //
                // This can happen if we have a lot of data coming into the store
                // within short periods of time, which means that the flush tasks
                // cannot keep up with the influx of data.
                //
                // In order to prevent the JVM from going out of memory, we have
                // no other choice but to defer (stall) the insert from happening
                // until the flush tasks have freed enough RAM.
                this.actualForestSizeReducedCondition.awaitUninterruptibly()
            }
            if (stallBegin >= 0) {
                val stallEnd = System.currentTimeMillis()
                val writeStallTime = stallEnd - stallBegin
                ChronoStoreStatistics.TOTAL_WRITE_STALL_TIME_MILLIS.addAndGet(writeStallTime)
                ChronoStoreStatistics.WRITE_STALL_EVENTS.incrementAndGet()
                log.perfTrace {
                    "Write to '${tree.path}' will no longer be stalled and may continue." +
                        " Stall time: ${writeStallTime}ms."
                }
            }

            val stats = getTreeStats(tree)
            stats.virtualTreeSize += bytesToInsert
            stats.actualTreeSize += bytesToInsert
            this.actualForestSize += bytesToInsert
            this.virtualForestSize += bytesToInsert

            // if we exceed the threshold, we need to schedule a flush task.
            while (this.virtualForestSize >= this.flushThresholdSize) {
                // schedule a flush task for the largest tree
                val (largestTree, largestTreeStats) = this.treeStats.entries.maxBy { it.key.inMemorySize }
                if(largestTreeStats.virtualTreeSize <= 0){
                    // all trees have a flush task scheduled, we just have to wait
                    break
                }
                val flushTask = FlushInMemoryTreeToDiskTask(largestTree, this.manifestFile)
                this.asyncTaskManager.executeAsync(flushTask)
                // we assume here that the tree will have 0 bytes in-memory after the flush.
                val bytesVirtuallyFreed = largestTreeStats.virtualTreeSize
                largestTreeStats.virtualTreeSize = 0
                // reduce the virtualForestSize by the bytes we are going to free
                // by flushing the tree. Note that we do NOT reduce the ACTUAL
                // forest size yet; that happens when the flush is completed.
                this.virtualForestSize -= bytesVirtuallyFreed
            }
        }
    }

    fun onInMemoryFlush(tree: LSMTree) {
        this.lock.write {
            val stats = getTreeStats(tree)
            val removedBytes = stats.actualTreeSize - tree.inMemorySize.bytes
            if (removedBytes <= 0) {
                // safeguard: if no data was actually flushed from the tree,
                // there's no need to do anything; in particular, there is
                // no need to wake up waiting writer threads.
                return
            }
            stats.actualTreeSize -= removedBytes
            stats.virtualTreeSize = min(stats.virtualTreeSize, stats.actualTreeSize)
            this.actualForestSize -= removedBytes
            this.virtualForestSize = min(this.virtualForestSize, this.actualForestSize)

            // Let waiting writers know that the forest size has been reduced.
            // This allows waiting writers to continue their task. We use "signalAll()"
            // here because we don't know a priori if we have enough space for all
            // of their data or just for some of it. The writers will check first
            // if space is available before the insertion, so "signalAll()" is correct here.
            this.actualForestSizeReducedCondition.signalAll()
        }
    }


    fun flushAllInMemoryStoresToDisk(taskMonitor: TaskMonitor = TaskMonitor.create()) {
        taskMonitor.mainTask("Flushing all in-memory stores to disk") {
            val futures = this.lock.write {
                this.treeStats.mapNotNull { (tree, treeStats) ->
                    if (treeStats.virtualTreeSize > 0) {
                        treeStats.virtualTreeSize = 0
                        val flushTask = FlushInMemoryTreeToDiskTask(tree, this.manifestFile)
                        this.asyncTaskManager.executeAsync(flushTask)
                    } else {
                        log.trace { "SKIP FLUSH; NO DATA in tree '${tree.path}'" }
                        null
                    }
                }
            }
            taskMonitor.forEach(1.0, "Waiting for flush tasks to complete", futures, Future<TaskExecutionResult>::get)
        }
    }

    private fun getTreeStats(tree: LSMTree): TreeStats {
        return this.treeStats[tree]
            ?: throw IllegalStateException("No stats tracked for LSM Tree '${tree.path}'! It seems not to be part of this forest!")
    }

    private class TreeStats(
        var virtualTreeSize: Long,
        var actualTreeSize: Long,
    ) {

        constructor(tree: LSMTree) : this(tree.inMemorySize.bytes, tree.inMemorySize.bytes)

    }

}