package org.example.dbfromzero.io.lsm

import mu.KotlinLogging
import org.chronos.chronostore.io.vfs.VirtualDirectory
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualDirectory
import org.chronos.chronostore.io.vfs.disk.DiskBasedVirtualFileSystem
import org.chronos.chronostore.io.vfs.inmemory.InMemoryVirtualFileSystem
import org.chronos.chronostore.util.Bytes
import java.io.File
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class LsmTree private constructor(
    private val pendingWritesDeltaThreshold: Int,
    private val directoryManager: DirectoryManager,
    private val coordinator: DeltaWriterCoordinator,
    private val mergerCron: BaseDeltaMergerCron,
    private val executorService: ExecutorService,
) : ReadWriteStorage {

    companion object {

        private val log = KotlinLogging.logger {}

        val DELETE_VALUE: Bytes = Bytes(
            byteArrayOf(
                83, 76, 69, 7, 95, 21, 81, 27, 2, 104, 8, 100, 45, 109, 110, 1
            )
        )

        fun builder(): Builder {
            return Builder()
        }

        fun builderForDirectory(directory: VirtualDirectory): Builder {
            val builder = builder()
            builder.withDirectoryManagerOn(directory)
            return builder
        }

        fun builderForOnDiskDirectory(directory: File): Builder {
            require(directory.exists() && directory.isDirectory) {
                "Argument 'directory' must refer to an existing directory, but it does not. Path: ${directory.absolutePath}"
            }
            val vfs = DiskBasedVirtualFileSystem(directory)
            val virtualDirectory = vfs.directory("lsm")
            return builderForDirectory(virtualDirectory)
        }

        fun builderForInMemoryDirectory(name: String): Builder {
            require(name.isNotEmpty()) {
                "Argument 'name' must not be empty!"
            }
            val vfs = InMemoryVirtualFileSystem()
            val directory = vfs.directory(name)
            directory.mkdirs()
            return builderForDirectory(directory)
        }
    }

    class Builder {
        private var pendingWritesDeltaThreshold = 10_000
        private var directoryManager: DirectoryManager? = null
        private var deltaWriterCoordinator: DeltaWriterCoordinator? = null
        private var mergerCron: BaseDeltaMergerCron? = null
        private var executorService: ScheduledExecutorService? = null

        private var indexRate = 10
        private var maxInFlightWriteJobs = 10
        private var maxDeltaReadPercentage = 0.5
        private var mergeCronFrequency = Duration.ofSeconds(1)

        fun withPendingWritesDeltaThreshold(pendingWritesDeltaThreshold: Int): Builder {
            require(pendingWritesDeltaThreshold > 0) { "Argument 'pendingWritesDeltaThreshold' must be greater than 0, but got: $pendingWritesDeltaThreshold" }
            this.pendingWritesDeltaThreshold = pendingWritesDeltaThreshold
            return this
        }

        fun withDirectoryManager(directoryManager: DirectoryManager): Builder {
            this.directoryManager = directoryManager
            return this
        }

        fun withDirectoryManagerOn(directory: VirtualDirectory): Builder {
            return withDirectoryManager(DirectoryManager(directory))
        }

        fun withDeltaWriteCoordinator(coordinator: DeltaWriterCoordinator): Builder {
            deltaWriterCoordinator = coordinator
            return this
        }

        fun withMergerCron(mergerCron: BaseDeltaMergerCron): Builder {
            this.mergerCron = mergerCron
            return this
        }

        fun withScheduledExecutorService(executorService: ScheduledExecutorService): Builder {
            this.executorService = executorService
            return this
        }

        fun withScheduledExecutorServiceOfSize(corePoolSize: Int): Builder {
            return withScheduledExecutorService(Executors.newScheduledThreadPool(corePoolSize))
        }

        fun withIndexRate(indexRate: Int): Builder {
            require(indexRate > 0) { "Argument 'indexRate' must be greater than 0, but got: $indexRate" }
            this.indexRate = indexRate
            return this
        }

        fun withMaxInFlightWriteJobs(maxInFlightWriteJobs: Int): Builder {
            require(maxInFlightWriteJobs > 0) { "Argument 'maxInFlightWriteJobs' must be greater than 0, but got: $maxInFlightWriteJobs" }
            this.maxInFlightWriteJobs = maxInFlightWriteJobs
            return this
        }

        fun withMaxDeltaReadPercentage(maxDeltaReadPercentage: Double): Builder {
            require(maxDeltaReadPercentage > 0) { "Argument 'maxDeltaReadPercentage' must be greater than 0, but got: $maxDeltaReadPercentage" }
            require(maxDeltaReadPercentage < 1) { "Argument 'maxDeltaReadPercentage' must be less than 1, but got: $maxDeltaReadPercentage" }
            this.maxDeltaReadPercentage = maxDeltaReadPercentage
            return this
        }

        fun withMergeCronFrequency(mergeCronFrequency: Duration): Builder {
            require(!mergeCronFrequency.isZero) { "Argument 'mergeCronFrequency' must not be zero, but got: $mergeCronFrequency" }
            require(!mergeCronFrequency.isNegative) { "Argument 'mergeCronFrequency' must not be negative, but got: $mergeCronFrequency" }
            this.mergeCronFrequency = mergeCronFrequency
            return this
        }

        fun build(): LsmTree {
            val directoryManager = checkNotNull(this.directoryManager) {
                "LSM Tree Builder did not specify the directory manager!"
            }
            val executorService = this.executorService
                ?: Executors.newScheduledThreadPool(4)
            val coordinator = this.deltaWriterCoordinator
                ?: DeltaWriterCoordinator(
                    directoryManager = directoryManager,
                    indexRate = this.indexRate,
                    maxInFlightWriters = this.maxInFlightWriteJobs,
                    executor = executorService
                )
            val mergerCron = this.mergerCron
                ?: BaseDeltaMergerCron(
                    directoryManager = directoryManager,
                    maxDeltaReadPercentage = this.maxDeltaReadPercentage,
                    checkForDeltaRate = this.mergeCronFrequency,
                    baseIndexRate = this.indexRate,
                    executor = executorService
                )
            return LsmTree(
                pendingWritesDeltaThreshold = pendingWritesDeltaThreshold,
                directoryManager = directoryManager,
                coordinator = coordinator,
                mergerCron = mergerCron,
                executorService = executorService
            )
        }
    }

    private val lock = ReentrantReadWriteLock(true)
    private var pendingWrites: MutableMap<Bytes, Bytes>? = mutableMapOf()

    fun initialize() {
        this.mergerCron.start()
    }

    override fun close() {
        this.mergerCron.shutdown()
        // Avoid clean shutdown for now since just running for benchmarking
        /*
        if (isUsable()) {
          lock.write {
            if (!pendingWrites.isEmpty()) {
              if (coordinator.hasMaxInFlightWriters()) {
                log.warn { "Delaying close because DeltaWriteCoordinator is backed up" }
                waitForWritesToUnblock()
              }
              sendWritesToCoordinator()
            }
            pendingWrites = null
          }
        }
         */
        executorService.shutdownNow()
        try {
            val terminated = executorService.awaitTermination(10, TimeUnit.SECONDS)
            check(terminated) { "Executor service could not shut down within the time limit!" }
        } catch (e: InterruptedException) {
            throw RuntimeException("Shutdown was interrupted!", e)
        }
    }

    override val size: Int
        get() = throw UnsupportedOperationException("not implemented yet")

    val isUsable: Boolean
        get() = !this.coordinator.anyWritesAborted && !mergerCron.hasErrors

    override fun put(key: Bytes, value: Bytes) {
        check(this.isUsable)
        val writesBlocked = this.lock.write {
            val pendingWrites = checkNotNull(this.pendingWrites) {
                "This LSM-Tree is already closed."
            }

            pendingWrites[key] = value
            checkMergeThreshold()
        }
        if (writesBlocked) {
            waitForWritesToUnblock()
        }
    }

    override fun get(key: Bytes): Bytes? {
        check(this.isUsable)
        println("GET::Key=${key}")
        val value = this.lock.read {
            checkNotNull(this.pendingWrites) {
                "This LSM-Tree is already closed."
            }[key]
        }
        if (value != null) {
            return checkForDeleteValue(value)
        }
        val valueFromCoordinator = coordinator.searchForKeyInWritesInProgress(key)
        if (valueFromCoordinator != null) {
            return checkForDeleteValue(valueFromCoordinator)
        }
        if (!this.directoryManager.isBaseInUse()) {
            return null // tree is empty
        }
        val valueFromBase = this.directoryManager.searchForKey(key)
        return checkForDeleteValue(valueFromBase)
    }

    private fun checkForDeleteValue(value: Bytes?): Bytes? {
        if (value == null || value == DELETE_VALUE) {
            println("GET::Result: NULL <deleted>")
            return null
        }
        println("GET::Result: ${value}")
        return value
    }

    override fun delete(key: Bytes): Boolean {
        check(this.isUsable)
        println("DEL::Key=${key}")
        val writesBlocked = lock.write {
            checkNotNull(this.pendingWrites) {
                "This LSM-Tree is already closed."
            }[key] = DELETE_VALUE
            checkMergeThreshold()
        }
        if (writesBlocked) {
            waitForWritesToUnblock()
        }
        // not really useful
        return true
    }

    private fun checkMergeThreshold(): Boolean {
        if ((pendingWrites?.size ?: 0) < this.pendingWritesDeltaThreshold) {
            // not enough writes yet to flush
            return false
        }
        if (this.coordinator.hasMaxInFlightWriters) {
            // we would like to write, but no writer threads are available
            return true
        }
        // hand over our writes to the coordinator (to write later)...
        sendWritesToCoordinator()
        // ... and clear the map
        this.pendingWrites = mutableMapOf()
        return false
    }

    private fun sendWritesToCoordinator() {
        this.pendingWrites?.let(this.coordinator::addWrites)
    }

    private fun waitForWritesToUnblock() {
        log.warn { "There are too many in-flight write jobs. Waiting for them to finish." }
        try {
            while (coordinator.hasMaxInFlightWriters) {
                Thread.sleep(5000)
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw RuntimeException("Interrupted while waiting for write jobs to finish!")
        }
        lock.write(this::sendWritesToCoordinator)
    }


}