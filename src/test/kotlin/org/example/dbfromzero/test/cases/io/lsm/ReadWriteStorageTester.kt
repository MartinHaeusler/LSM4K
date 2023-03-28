package org.example.dbfromzero.test.cases.io.lsm

import mu.KotlinLogging
import org.example.dbfromzero.io.lsm.ReadWriteStorage
import org.example.dbfromzero.util.Bytes
import org.example.dbfromzero.util.LittleEndianUtil
import java.io.IOException
import java.util.*
import java.util.function.Consumer
import java.util.function.Supplier
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue


class ReadWriteStorageTester {

    companion object {

        private val log = KotlinLogging.logger {}

        fun builder(storage: ReadWriteStorage): Builder {
            return Builder(TestAdapter(storage))
        }

        fun builderForIntegers(
            storage: ReadWriteStorage,
            random: Random,
            keySize: Int
        ): Builder {
            return builder(storage)
                .knownKeySupplier { Bytes(LittleEndianUtil.writeLittleEndianInt(random.nextInt(keySize))) }
                .unknownKeySupplier { Bytes(LittleEndianUtil.writeLittleEndianInt(keySize + random.nextInt(keySize))) }
                .valueSupplier { Bytes(LittleEndianUtil.writeLittleEndianInt(random.nextInt())) }
                .random(random)
        }

        fun builderForIntegers(
            storage: ReadWriteStorage,
            seed: RandomSeed,
            keySetSize: KeySetSize
        ): Builder {
            return builderForIntegers(storage, seed.random(), keySetSize.size)
        }

        fun builderForBytes(
            storage: ReadWriteStorage,
            random: Random,
            keySize: Int,
            valueSize: Int
        ): Builder {
            return builder(storage)
                .knownKeySupplier { Bytes.random(random, keySize) }
                .unknownKeySupplier { Bytes.random(random, keySize + 1) }
                .valueSupplier { Bytes.random(random, valueSize) }
                .random(random)
        }

        fun <T> runCallback(callback: Consumer<T>, x: T) {
            try {
                callback.accept(x)
            } catch (e: IOException) {
                throw AssertionError("unexpected IOError in callback", e)
            }
        }

    }

    class Builder(adapter: TestAdapter) {
        val adapter: TestAdapter
        var knownKeySupplier: Supplier<Bytes>? = null
        var unknownKeySupplier: Supplier<Bytes>? = null
        var valueSupplier: Supplier<Bytes>? = null
        var iterationCallback: Consumer<ReadWriteStorage>? = null
        var random: Random? = null
        var debug = false
        var checkSize = true
        var checkDeleteReturnValue = true

        init {
            this.adapter = adapter
        }

        fun knownKeySupplier(knownKeySupplier: Supplier<Bytes>): Builder {
            this.knownKeySupplier = knownKeySupplier
            return this
        }

        fun unknownKeySupplier(unknownKeySupplier: Supplier<Bytes>): Builder {
            this.unknownKeySupplier = unknownKeySupplier
            return this
        }

        fun valueSupplier(valueSupplier: Supplier<Bytes>): Builder {
            this.valueSupplier = valueSupplier
            return this
        }

        fun iterationCallback(iterationCallback: Consumer<ReadWriteStorage>): Builder {
            this.iterationCallback = iterationCallback
            return this
        }

        fun random(random: Random): Builder {
            this.random = random
            return this
        }

        fun debug(debug: Boolean): Builder {
            this.debug = debug
            return this
        }

        fun checkSize(checkSize: Boolean): Builder {
            this.checkSize = checkSize
            return this
        }

        fun checkDeleteReturnValue(checkDeleteReturnValue: Boolean): Builder {
            this.checkDeleteReturnValue = checkDeleteReturnValue
            return this
        }

        fun build(): ReadWriteStorageTester {
            return ReadWriteStorageTester(this)
        }
    }

    class TestAdapter constructor(
        val storage: ReadWriteStorage
    ) {

        val size: Int
            get() {
                return try {
                    storage.size
                } catch (e: IOException) {
                    throw AssertionError("unexpected IOError in size() of $storage", e)
                }
            }

        operator fun set(key: Bytes, value: Bytes) {
            this.put(key, value)
        }

        fun put(key: Bytes, value: Bytes) {
            try {
                storage.put(key, value)
            } catch (e: IOException) {
                throw AssertionError("unexpected IOError for put() ${key}=${value} of $storage", e)
            }
        }

        operator fun get(key: Bytes): Bytes? {
            return try {
                storage.get(key)
            } catch (e: IOException) {
                throw AssertionError("unexpected IOError for get() ${key} of ${storage}", e)
            }
        }

        fun delete(key: Bytes): Boolean {
            return try {
                storage.delete(key)
            } catch (e: IOException) {
                throw AssertionError("unexpected IOError for delete() ${key} of ${storage}", e)
            }
        }
    }


    private var adapter: TestAdapter
    private var knownKeySupplier: Supplier<Bytes>
    private var unknownKeySupplier: Supplier<Bytes>
    private var valueSupplier: Supplier<Bytes>
    private var iterationCallback: Consumer<ReadWriteStorage>
    private var random: Random
    private var debug = false
    private var checkSize = false
    private var checkDeleteReturnValue = false

    constructor(builder: Builder) {
        adapter = builder.adapter
        knownKeySupplier = checkNotNull(builder.knownKeySupplier)
        unknownKeySupplier = checkNotNull(builder.unknownKeySupplier)
        valueSupplier = checkNotNull(builder.valueSupplier)
        iterationCallback = builder.iterationCallback ?: Consumer { }
        random = Optional.ofNullable(builder.random).orElseGet { Random() }
        debug = builder.debug
        checkSize = builder.checkSize
        checkDeleteReturnValue = builder.checkDeleteReturnValue
    }

    fun testAddDeleteMany(count: Int) {
        val map = mutableMapOf<Bytes, Bytes>()
        repeat(count) {
            doPut(map, knownKeySupplier.get())
            if (checkSize) {
                assertEquals(map.size, adapter.size)
            }
            runCallback(iterationCallback, adapter.storage)
        }
        map.forEach { (key, value) -> assertEquals(value, adapter[key]) }
        for (key in generateSequence(unknownKeySupplier::get).take(count)) {
            assertNull(adapter[key])
        }

        for (key in generateSequence(unknownKeySupplier::get).take(count)) {
            doDelete(null, false, key)
        }
        for (key in map.keys) {
            doDelete(null, true, key)
        }
        if (checkSize) {
            assertEquals(0, adapter.size)
        }
    }

    fun testPutDeleteGet(count: Int, putDeleteGet: PutDeleteGet, knownKeyRate: KnownKeyRate) {
        val map = mutableMapOf<Bytes, Bytes>()
        val existingKeySupplier = Supplier<Bytes> { map.keys.asSequence().drop(random.nextInt(map.size)).first() }
        repeat(count) {
            val known = map.isNotEmpty() && random.nextDouble() < knownKeyRate.rate
            var r = random.nextDouble()
            if (r < putDeleteGet.putRate) {
                val key = if (known) {
                    existingKeySupplier.get()
                } else {
                    knownKeySupplier.get()
                }
                check(!known || map.containsKey(key))
                doPut(map, key)
            } else {
                val key = if (known) {
                    existingKeySupplier.get()
                } else {
                    unknownKeySupplier.get()
                }
                check(known == map.containsKey(key))
                r -= putDeleteGet.putRate
                if (r < putDeleteGet.getRate) {
                    doGet(map, known, key)
                } else {
                    doDelete(map, known, key)
                }
            }
            if (checkSize) {
                assertEquals(map.size, adapter.size)
            }
            runCallback(iterationCallback, adapter.storage)
        }
    }

    private fun doDelete(map: MutableMap<Bytes, Bytes>?, known: Boolean, key: Bytes) {
        log.trace { "del $key" }
        if (known) {
            map?.remove(key)
            if (checkDeleteReturnValue) {
                assertTrue(adapter.delete(key))
            }
        } else {
            if (checkDeleteReturnValue) {
                assertFalse(adapter.delete(key))
            }
        }
    }

    private fun doGet(map: MutableMap<Bytes, Bytes>, known: Boolean, key: Bytes) {
        log.trace { "get $key" }
        if (known) {
            assertEquals(map[key], adapter[key])
        } else {
            assertNull(adapter[key])
        }
    }

    private fun doPut(map: MutableMap<Bytes, Bytes>, key: Bytes) {
        val value = this.valueSupplier.get()
        log.trace { "put $key=$value" }
        map[key] = value
        adapter[key] = value
    }
}