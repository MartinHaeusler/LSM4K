package org.lsm4k.util.sequence

import java.util.*

object SequenceExtensions {

    @JvmName("mapEntriesToNavigableMap")
    fun <K : Comparable<K>, V> Sequence<Map.Entry<K, V>>.toTreeMap(): NavigableMap<K, V> {
        val treeMap = TreeMap<K, V>()
        for ((key, value) in this) {
            treeMap.put(key, value)
        }
        return treeMap
    }

    @JvmName("pairsToNavigableMap")
    fun <K : Comparable<K>, V> Sequence<Pair<K, V>>.toTreeMap(): NavigableMap<K, V> {
        val treeMap = TreeMap<K, V>()
        return this.toMap(treeMap)
    }

}