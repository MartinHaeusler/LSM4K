package org.chronos.chronostore.io.format.datablock

import org.chronos.chronostore.io.fileaccess.RandomFileAccessDriver
import org.chronos.chronostore.io.format.BlockMetaData
import org.chronos.chronostore.io.format.ChronoStoreFileFormat
import org.chronos.chronostore.io.format.CompressionAlgorithm
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.LittleEndianExtensions.readLittleEndianInt
import org.chronos.chronostore.util.cursor.Cursor
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.util.*


interface DataBlock {

    val metaData: BlockMetaData

    fun isEmpty(): Boolean

    /**
     * Attempts to get the value for the given [key] from this block.
     *
     * @param key The key-and-timestamp to get from the block.
     *
     * @return Either the successful result, or `null` if the key wasn't found at all.
     *         If the key was found, a pair is returned. The command is the command
     *         associated with the largest key in the block which is less than or equal
     *         to the requested key. The value is a boolean which indicates if the
     *         returned key is the **last** key in the block. If this happens to be
     *         the case, the next data block needs to be consulted as well, because
     *         the entry we returned may not be the **latest** entry for the key.
     */
    fun get(key: KeyAndTimestamp, driver: RandomFileAccessDriver): Pair<Command, Boolean>?

    /**
     * Opens a new cursor on this block.
     *
     * The cursor needs to be closed explicitly.
     *
     * @return the new cursor.
     */
    fun cursor(driver: RandomFileAccessDriver): Cursor<KeyAndTimestamp, Command>

    companion object {

        @JvmStatic
        fun createEagerLoadingInMemoryBlock(
            inputStream: InputStream,
            compressionAlgorithm: CompressionAlgorithm
        ): DataBlock {
            val magicBytes = Bytes(inputStream.readNBytes(ChronoStoreFileFormat.BLOCK_MAGIC_BYTES.size))
            if (magicBytes != ChronoStoreFileFormat.BLOCK_MAGIC_BYTES) {
                throw IllegalArgumentException(
                    "Cannot read block from input: the magic bytes do not match!" +
                        " Expected ${ChronoStoreFileFormat.BLOCK_MAGIC_BYTES.hex()}, found ${magicBytes.hex()}!"
                )
            }
            // read the individual parts of the binary format
            inputStream.readLittleEndianInt() // block size; not needed here
            val blockMetadataSize = inputStream.readLittleEndianInt()
            val blockMetadataBytes = inputStream.readNBytes(blockMetadataSize)

            val bloomFilterSize = inputStream.readLittleEndianInt()
            // skip the bloom filter, we don't need it for eager-loaded blocks
            inputStream.skipNBytes(bloomFilterSize.toLong())

            val compressedSize = inputStream.readLittleEndianInt()
            val compressedBytes = inputStream.readNBytes(compressedSize)

            // deserialize the binary representations
            val blockMetaData = BlockMetaData.readFrom(ByteArrayInputStream(blockMetadataBytes))

            // decompress the data
            val decompressedData = compressionAlgorithm.decompress(compressedBytes)

            // performance optimization:
            // We want to construct a TreeMap from all our keys and values. The default
            // way to do this is to create an empty TreeMap and call put(...) for each
            // entry. However, this is inefficient, because put(...) does not assume
            // that the entries arrive in-order and therefore performs a lot of
            // processing to ensure the correct order. In this place, we can
            // *guarantee* that the entries will arrive in order, because it is part of
            // the file format specification. There is an optimized constructor for
            // TreeMap which requires a SortedMap as input. We create a "fake" sorted
            // map here from our input which ONLY implements the methods needed for the
            // optimized tree map constructor (entrySet().iterator() and size()).

            val fakeSortedMap = FakeSortedMap<KeyAndTimestamp, Command>()

            decompressedData.inputStream().use { byteIn ->
                while (byteIn.available() > 0) {
                    val command = Command.readFromStream(byteIn)
                    fakeSortedMap[command.keyAndTimestamp] = command
                }
            }

            val commands = TreeMap(fakeSortedMap)

            return EagerDataBlock(
                metaData = blockMetaData,
                data = commands
            )
        }

    }


    private class FakeSortedMap<K : Comparable<K>, V> : SortedMap<K, V> {

        private val data = mutableListOf<MutableMap.MutableEntry<K, V>>()

        override fun containsKey(key: K): Boolean {
            return this.data.any { it.key == key }
        }

        override fun containsValue(value: V): Boolean {
            return this.data.any { it.value == value }
        }

        override fun get(key: K): V? {
            return this.entries.firstOrNull { it.key == key }?.value
        }

        override fun clear() {
            return this.data.clear()
        }

        override fun remove(key: K): V? {
            val iterator = this.data.iterator()
            if (iterator.hasNext()) {
                val entry = iterator.next()
                if (entry.key == key) {
                    iterator.remove()
                    return entry.value
                }
            }
            return null
        }

        override fun putAll(from: Map<out K, V>) {
            for (entry in from) {
                this[entry.key] = entry.value
            }
        }

        override fun put(key: K, value: V): V? {
            // DELIBERATELY no check for containment here!
            this.data.add(ImmutableEntry(key, value))
            // deliberate performance optimization
            return null
        }

        override fun isEmpty(): Boolean {
            return this.data.isEmpty()
        }

        override fun comparator(): Comparator<in K> {
            return Comparator.naturalOrder()
        }

        override fun firstKey(): K {
            return this.data.first().key
        }

        override fun lastKey(): K {
            return this.data.last().key
        }

        override val entries: MutableSet<MutableMap.MutableEntry<K, V>>
            get() = FakeEntrySet()

        override val keys: MutableSet<K>
            get() = throw UnsupportedOperationException("Operation 'keys' is not supported.")

        override val values: MutableCollection<V>
            get() = throw UnsupportedOperationException("Operation 'values' is not supported.")

        override val size: Int
            get() = this.data.size

        override fun tailMap(fromKey: K): SortedMap<K, V> {
            throw UnsupportedOperationException("Operation 'keys' is not supported.")
        }

        override fun headMap(toKey: K): SortedMap<K, V> {
            throw UnsupportedOperationException("Operation 'keys' is not supported.")
        }

        override fun subMap(fromKey: K, toKey: K): SortedMap<K, V> {
            throw UnsupportedOperationException("Operation 'keys' is not supported.")
        }

        private class ImmutableEntry<K, V>(
            override val key: K,
            override var value: V,
        ) : MutableMap.MutableEntry<K, V> {

            override fun setValue(newValue: V): V {
                val oldValue = this.value
                this.value = newValue
                return oldValue
            }

        }

        private inner class FakeEntrySet : MutableSet<MutableMap.MutableEntry<K, V>> {
            override fun add(element: MutableMap.MutableEntry<K, V>): Boolean {
                throw UnsupportedOperationException("Operation 'add' is not supported!")
            }

            override fun addAll(elements: Collection<MutableMap.MutableEntry<K, V>>): Boolean {
                throw UnsupportedOperationException("Operation 'add' is not supported!")
            }

            override val size: Int
                get() = this@FakeSortedMap.size

            override fun clear() {
                throw UnsupportedOperationException("Operation 'add' is not supported!")
            }

            override fun isEmpty(): Boolean {
                return this.size <= 0
            }

            override fun containsAll(elements: Collection<MutableMap.MutableEntry<K, V>>): Boolean {
                throw UnsupportedOperationException("Operation 'add' is not supported!")
            }

            override fun contains(element: MutableMap.MutableEntry<K, V>): Boolean {
                throw UnsupportedOperationException("Operation 'add' is not supported!")
            }

            override fun iterator(): MutableIterator<MutableMap.MutableEntry<K, V>> {
                return this@FakeSortedMap.data.iterator()
            }

            override fun retainAll(elements: Collection<MutableMap.MutableEntry<K, V>>): Boolean {
                throw UnsupportedOperationException("Operation 'add' is not supported!")
            }

            override fun removeAll(elements: Collection<MutableMap.MutableEntry<K, V>>): Boolean {
                throw UnsupportedOperationException("Operation 'add' is not supported!")
            }

            override fun remove(element: MutableMap.MutableEntry<K, V>): Boolean {
                throw UnsupportedOperationException("Operation 'add' is not supported!")
            }

        }

    }
}
