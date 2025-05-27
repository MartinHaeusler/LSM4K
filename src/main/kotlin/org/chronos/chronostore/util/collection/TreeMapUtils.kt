package org.chronos.chronostore.util.collection

import java.util.*

object TreeMapUtils {

    fun <K : Comparable<K>, V> treeMapOf(): TreeMap<K, V> {
        return TreeMap()
    }

    fun <K : Comparable<K>, V> treeMapOf(vararg entries: Pair<K, V>): TreeMap<K, V> {
        val map = TreeMap<K, V>()
        for (entry in entries) {
            map[entry.first] = entry.second
        }
        return map
    }

}