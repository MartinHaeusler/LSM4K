package org.example.dbfromzero.test.cases.io.lsm

import com.google.common.collect.Streams
import mu.KotlinLogging
import org.example.dbfromzero.io.lsm.LsmTree
import org.example.dbfromzero.io.vfs.VirtualDirectory
import org.example.dbfromzero.io.vfs.inmemory.InMemoryFileSystem
import org.example.dbfromzero.util.Bytes
import org.junit.jupiter.api.Test
import org.opentest4j.AssertionFailedError
import java.lang.AssertionError
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors
import java.util.stream.IntStream
import java.util.stream.Stream
import kotlin.test.assertEquals


class LsmTreeTest {

    private companion object {

        private val log = KotlinLogging.logger {}

    }

    @Test
    fun testSingleThreaded() {
        val (directory, tree) = createInMemoryLsmTree(1000)
        val builder = ReadWriteStorageTester.builderForBytes(tree, RandomSeed.CAFE.random(), 16, 4096)
            .debug(true)
            .checkDeleteReturnValue(false)
            .checkSize(false)
        val count = AtomicInteger(0)
        builder.iterationCallback {
            if (count.incrementAndGet() % 1000 == 0) {
                log.info { "iteration " + count.get() + " size " + Bytes.formatSize(getDirectorySize(directory)) }
            }
        }
        val tester = builder.build()
        tester.testPutDeleteGet(10 * 1000, PutDeleteGet.BALANCED, KnownKeyRate.MID)
    }

    @Test
    @Throws(java.lang.Exception::class)
    fun testMultiThread() {
        val (directory, tree) = createInMemoryLsmTree(5 * 1000)
        val errors = AtomicInteger(0)
        val threads = Streams.concat(
            Stream.of(
                createThread(tree, PutDeleteGet.PUT_HEAVY, KnownKeyRate.LOW, true, errors, directory),
                createThread(tree, PutDeleteGet.DELETE_HEAVY, KnownKeyRate.HIGH, false, errors, directory)
            ),
            IntStream.range(0, 8).mapToObj { _ -> createThread(tree, PutDeleteGet.GET_HEAVY, KnownKeyRate.HIGH, false, errors, directory) }).collect(Collectors.toList())
        threads.forEach { obj: Thread -> obj.start() }
        for (thread in threads) {
            thread.join()
        }
        assertEquals(0, errors.get())
    }

    private fun createInMemoryLsmTree(pendingWritesDeltaThreshold: Int): Pair<VirtualDirectory, LsmTree> {
        val vfs = InMemoryFileSystem()
        val directory = vfs.directory("test")
        val tree = LsmTree.builderForDirectory(directory)
            .withPendingWritesDeltaThreshold(pendingWritesDeltaThreshold)
            .withScheduledExecutorServiceOfSize(2)
            .withIndexRate(10)
            .withMaxInFlightWriteJobs(10)
            .withMaxDeltaReadPercentage(0.5)
            .withMergeCronFrequency(Duration.ofMillis(100))
            .build()
        tree.initialize()
        return Pair(directory, tree)
    }

    private fun getDirectorySize(d: VirtualDirectory): Long {
        return d.list().asSequence().map(d::file).map { it.length }.sum()
    }

    private fun createThread(
        tree: LsmTree,
        putDeleteGet: PutDeleteGet,
        knownKeyRate: KnownKeyRate,
        callback: Boolean,
        errors: AtomicInteger,
        virtualDirectory: VirtualDirectory
    ): Thread {
        val builder = ReadWriteStorageTester.builderForBytes(tree, Random(), 16, 4096)
            .debug(false)
            .checkSize(false)
            .checkDeleteReturnValue(false)
        if (callback) {
            val count = AtomicInteger(0)
            builder.iterationCallback {
                assertEquals(0, errors.get())
                if (count.incrementAndGet() % 1000 == 0) {
                    log.info { "iteration " + count.get() + " size " + Bytes.formatSize(getDirectorySize(virtualDirectory)) }
                }
            }
        } else {
            builder.iterationCallback { assertEquals(0, errors.get()) }
        }
        val tester = builder.build()
        return Thread {
            try {
                tester.testPutDeleteGet(25 * 1000, putDeleteGet, knownKeyRate)
            } catch (e: AssertionFailedError) {
                log.error(e) { "error in thread" }
                errors.incrementAndGet()
            } catch (e: AssertionError) {
                log.error(e) { "error in thread" }
                errors.incrementAndGet()
            } catch (e: Exception) {
                log.error(e) { "error in thread" }
                errors.incrementAndGet()
            }
        }
    }
}