package org.chronos.chronostore.jmh

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding.intToEntry
import jetbrains.exodus.bindings.StringBinding.stringToEntry
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.EnvironmentConfig
import jetbrains.exodus.env.Environments.newInstance
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING
import jetbrains.exodus.env.Transaction
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.BenchmarkParams
import org.openjdk.jmh.infra.Blackhole
import java.io.IOException
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.math.max


@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.SampleTime)
open class XodusBenchmark : Common() {
    @Benchmark
    fun readCrc(r: Reader, bh: Blackhole) {
        r.crc.reset()
        r.store.openCursor(r.tx).use { c ->
            while (c.next) {
                r.crc.update(c.key.bytesUnsafe, 0, r.keySize)
                r.crc.update(c.value.bytesUnsafe, 0, r.valSize)
            }
        }
        bh.consume(r.crc.value)
    }

    @Benchmark
    fun readKey(r: Reader, bh: Blackhole) {
        for (key in r.keys) {
            if (r.intKey) {
                val value = r.store.get(r.tx, intToEntry(key))
                if (value != null) {
                    bh.consume(value.bytesUnsafe)
                }
            } else {
                val value = r.store.get(r.tx, stringToEntry(r.padKey(key)))
                if (value != null) {
                    bh.consume(value.bytesUnsafe)
                }
            }
        }
    }

    @Benchmark
    fun readRev(r: Reader, bh: Blackhole) {
        r.store.openCursor(r.tx).use { c ->
            c.last
            do {
                bh.consume(c.value.bytesUnsafe)
            } while (c.prev)
        }
    }

    @Benchmark
    fun readSeq(r: Reader, bh: Blackhole) {
        r.store.openCursor(r.tx).use { c ->
            while (c.next) {
                bh.consume(c.value.bytesUnsafe)
            }
        }
    }

    @Benchmark
    fun readXxh64(r: Reader, bh: Blackhole) {
        var result: Long = 0
        r.store.openCursor(r.tx).use { c ->
            while (c.next) {
                result += c.key.hashCode()
                result += c.value.hashCode()
            }
        }
        bh.consume(result)
    }

    @Benchmark
    fun write(w: Writer, bh: Blackhole?) {
        w.write()
    }

    @State(value = Scope.Benchmark)
    open class CommonXodus : Common() {
        lateinit var env: Environment
        lateinit var store: Store

        override fun setup(b: BenchmarkParams) {
            super.setup(b)
            val cfg = EnvironmentConfig()
            // size of immutable .xd file is 32MB
            cfg.setLogFileSize(32 * 1024)
            cfg.setLogCachePageSize(0x20000)
            env = newInstance(tmp!!, cfg)
            env.executeInTransaction { txn: Transaction ->
                // WITHOUT_DUPLICATES_WITH_PREFIXING means Patricia tree is used,
                // not B+Tree (WITHOUT_DUPLICATES)
                // Patricia tree gives faster random access, both for reading and writing
                store = env.openStore(
                    "without_dups", WITHOUT_DUPLICATES_WITH_PREFIXING,
                    txn
                )
            }
        }

        override fun teardown() {
            reportSpaceBeforeClose()
            env.close()
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
                env.executeInTransaction { tx: Transaction ->
                    var i = 0
                    var j = keyStartIndex
                    while (i < batchSize && j < keys.size) {
                        val key: Int = keys[j]
                        val valBi: ByteIterable
                        val keyBi: ByteIterable = if (intKey) {
                            intToEntry(key)
                        } else {
                            stringToEntry(padKey(key))
                        }
                        if (valRandom) {
                            valBi = ArrayByteIterable(rbi.nextBytes())
                        } else {
                            val bytes = ByteArray(valSize)
                            bytes[0] = (key ushr 24).toByte()
                            bytes[1] = (key ushr 16).toByte()
                            bytes[2] = (key ushr 8).toByte()
                            bytes[3] = key.toByte()
                            valBi = ArrayByteIterable(bytes, valSize)
                        }
                        if (sequential) {
                            store.putRight(tx, keyBi, valBi)
                        } else {
                            store.put(tx, keyBi, valBi)
                        }
                        i++
                        j++
                    }
                }
            }
        }
    }

    @State(Scope.Benchmark)
    open class Reader : CommonXodus() {
        lateinit var tx: Transaction

        @Setup(Level.Trial)
        @Throws(IOException::class)
        override fun setup(b: BenchmarkParams) {
            super.setup(b)
            super.write()
            tx = env.beginReadonlyTransaction()
            // cannot share Cursor, as there's no Cursor.getFirst() to reset methods
        }

        @TearDown(Level.Trial)
        @Throws(IOException::class)
        override fun teardown() {
            tx.abort()
            super.teardown()
        }
    }

    @State(Scope.Benchmark)
    open class Writer : CommonXodus() {
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