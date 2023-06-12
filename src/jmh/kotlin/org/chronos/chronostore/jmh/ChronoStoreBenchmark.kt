package org.chronos.chronostore.jmh

import org.chronos.chronostore.api.ChronoStore
import org.chronos.chronostore.api.ChronoStoreConfiguration
import org.chronos.chronostore.api.ChronoStoreTransaction
import org.chronos.chronostore.api.Store
import org.chronos.chronostore.util.Bytes
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.BenchmarkParams
import org.openjdk.jmh.infra.Blackhole
import java.io.IOException
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.math.max
import kotlin.math.min


@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.SampleTime)
open class ChronoStoreBenchmark : Common() {
    @Benchmark
    fun readCrc(r: Reader, bh: Blackhole) {
        r.crc.reset()
        val txStore = r.tx.store(r.store.id)
        txStore.openCursorOnLatest().use { c ->
            c.firstOrThrow()
            do {
                r.crc.update(c.key.toByteArray(), 0, r.keySize)
                r.crc.update(c.value.toByteArray(), 0, min(c.value.size, r.valSize))
            } while (c.next())
        }
        bh.consume(r.crc.value)
    }

    @Benchmark
    fun readKey(r: Reader, bh: Blackhole) {
        val txStore = r.tx.store(r.store.id)
        for (key in r.keys) {
            if (r.intKey) {
                val value = txStore.getLatest(Bytes.stableInt(key))
                if (value != null) {
                    bh.consume(value)
                }
            } else {
                val value = txStore.getLatest(Bytes(r.padKey(key)))
                if (value != null) {
                    bh.consume(value)
                }
            }
        }
    }

    @Benchmark
    fun readRev(r: Reader, bh: Blackhole) {
        r.tx.store(r.store.id).openCursorOnLatest().use { c ->
            c.lastOrThrow()
            do {
                bh.consume(c.value)
            } while (c.previous())
        }
    }

    @Benchmark
    fun readSeq(r: Reader, bh: Blackhole) {
        r.tx.store(r.store.id).openCursorOnLatest().use { c ->
            c.firstOrThrow()
            do {
                bh.consume(c.value)
            } while (c.next())
        }
    }

    @Benchmark
    fun readXxh64(r: Reader, bh: Blackhole) {
        var result: Long = 0
        r.tx.store(r.store.id).openCursorOnLatest().use { c ->
            c.firstOrThrow()
            do {
                result += c.key.hashCode()
                result += c.value.hashCode()
            } while (c.next())
        }
        bh.consume(result)
    }

    @Benchmark
    fun write(w: Writer, bh: Blackhole?) {
        w.write()
    }

    @State(value = Scope.Benchmark)
    open class CommonChronoStore : Common() {
        lateinit var chronoStore: ChronoStore
        lateinit var store: Store

        override fun setup(b: BenchmarkParams) {
            super.setup(b)
            val cfg = ChronoStoreConfiguration()
            // size of immutable .xd file is 32MB
            cfg.maxBlockSizeInBytes = 32 * 1024 * 1024
            chronoStore = ChronoStore.openOnDirectory(tmp!!, cfg)
            chronoStore.transaction { txn ->
                store = txn.createNewStore("benchmarkstore", versioned = true).store
                txn.commit()
            }
        }

        override fun teardown() {
            reportSpaceBeforeClose()
            this.chronoStore.close()
            super.teardown()
        }

        fun write() {
            // optimal w/ valSize=16368 + default run
            val batchSize = max(1000000 / valSize, 1000)
            val rbi = RandomBytesIterator(valSize, this.RND_MB)
            var k = 0
            while (k < keys.size) {
                // write in several transactions so as not to block GC
                val keyStartIndex = k
                k += batchSize
                this.chronoStore.transaction { tx ->
                    var i = 0
                    var j = keyStartIndex
                    while (i < batchSize && j < keys.size) {
                        val key: Int = keys[j]
                        val valBi: Bytes
                        val keyBi: Bytes = if (intKey) {
                            Bytes.stableInt(key)
                        } else {
                            Bytes(padKey(key))
                        }
                        if (valRandom) {
                            valBi = Bytes(rbi.nextBytes())
                        } else {
                            val bytes = ByteArray(4)
                            bytes[0] = (key ushr 24).toByte()
                            bytes[1] = (key ushr 16).toByte()
                            bytes[2] = (key ushr 8).toByte()
                            bytes[3] = key.toByte()
                            valBi = Bytes(bytes)
                        }
                        if (sequential) {
                            tx.store(store.id).put(keyBi, valBi)
                        } else {
                            tx.store(store.id).put(keyBi, valBi)
                        }
                        i++
                        j++
                    }
                    tx.commit()
                }
            }
        }
    }

    @State(Scope.Benchmark)
    open class Reader : CommonChronoStore() {
        lateinit var tx: ChronoStoreTransaction

        @Setup(Level.Trial)
        @Throws(IOException::class)
        override fun setup(b: BenchmarkParams) {
            super.setup(b)
            super.write()
            tx = chronoStore.beginTransaction()
            // cannot share Cursor, as there's no Cursor.getFirst() to reset methods
        }

        @TearDown(Level.Trial)
        @Throws(IOException::class)
        override fun teardown() {
            tx.rollback()
            super.teardown()
        }
    }

    @State(Scope.Benchmark)
    open class Writer : CommonChronoStore() {
        @Setup(Level.Invocation)
        override fun setup(b: BenchmarkParams) {
            super.setup(b)
        }

        @TearDown(Level.Invocation)
        override fun teardown() {
            super.teardown()
        }
    }

    private class RandomBytesIterator(
        private val valSize: Int,
        private val RND_MB: ByteArray,
    ) {

        private var i = 0
        private val rndByteMax = RND_MB.size - valSize

        fun nextBytes(): ByteArray {
            val result: ByteArray = RND_MB.copyOfRange(i, valSize)
            i += valSize
            if (i >= rndByteMax) {
                i = 0
            }
            return result
        }
    }
}