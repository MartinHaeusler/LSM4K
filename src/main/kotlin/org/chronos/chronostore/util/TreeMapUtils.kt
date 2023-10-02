package org.chronos.chronostore.util

import java.util.*
import kotlin.Comparator

object TreeMapUtils {

    fun <K : Comparable<K>, V> treeMapFromSortedList(list: List<Map.Entry<K, V>>): TreeMap<K, V> {
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
        val fakeMap = FakeSortedMap(Comparator.naturalOrder(), list)
        return TreeMap(fakeMap)
    }


    private class FakeSortedMap<K, V>(
        private val comparator: Comparator<K>,
        private val data: List<Map.Entry<K, V>>
    ) : SortedMap<K, V> {

        override fun containsKey(key: K): Boolean {
            throw UnsupportedOperationException("Operation 'containsKey' is not supported.")
        }

        override fun containsValue(value: V): Boolean {
            throw UnsupportedOperationException("Operation 'containsValue' is not supported.")
        }

        override fun get(key: K): V? {
            throw UnsupportedOperationException("Operation 'get' is not supported.")
        }

        override fun clear() {
            throw UnsupportedOperationException("Operation 'clear' is not supported.")
        }

        override fun remove(key: K): V? {
            throw UnsupportedOperationException("Operation 'remove' is not supported.")
        }

        override fun putAll(from: Map<out K, V>) {
            throw UnsupportedOperationException("Operation 'putAll' is not supported.")
        }

        override fun put(key: K, value: V): V? {
            throw UnsupportedOperationException("Operation 'put' is not supported.")
        }

        override fun isEmpty(): Boolean {
            return this.data.isEmpty()
        }

        override fun comparator(): Comparator<in K> {
            return this.comparator
        }

        override fun firstKey(): K {
            throw UnsupportedOperationException("Operation 'firstKey' is not supported.")
        }

        override fun lastKey(): K {
            throw UnsupportedOperationException("Operation 'lastKey' is not supported.")
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
            throw UnsupportedOperationException("Operation 'tailMap' is not supported.")
        }

        override fun headMap(toKey: K): SortedMap<K, V> {
            throw UnsupportedOperationException("Operation 'headMap' is not supported.")
        }

        override fun subMap(fromKey: K, toKey: K): SortedMap<K, V> {
            throw UnsupportedOperationException("Operation 'subMap' is not supported.")
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

            @Suppress("UNCHECKED_CAST")
            override fun iterator(): MutableIterator<MutableMap.MutableEntry<K, V>> {
                return this@FakeSortedMap.data.iterator() as MutableIterator<MutableMap.MutableEntry<K, V>>
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