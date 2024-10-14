package org.chronos.chronostore.test.util

import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.IndexBasedCursor

object CursorTestUtils {

    fun <K : Comparable<K>, V> Map<K, V>.cursor(name: String = "Map Cursor"): Cursor<K, V> {
        val list = this.entries
            .sortedBy { it.key }
            .map { Pair(it.key, it.value) }

        return IndexBasedCursor(
            minIndex = 0,
            maxIndex = list.lastIndex,
            getEntryAtIndex = list::get,
            name = name,
        )
    }

    fun <K : Comparable<K>, V> Iterable<Pair<K, V>>.cursor(name: String = "List Cursor"): Cursor<K, V> {
        val list = this.sortedBy { it.first }
        return IndexBasedCursor(
            minIndex = 0,
            maxIndex = list.lastIndex,
            getEntryAtIndex = list::get,
            name = name,
        )
    }

    fun <K : Comparable<K>, V> cursorOn(vararg entries: Pair<K, V>): Cursor<K, V> {
        return cursorOn("List Cursor", *entries)
    }

    fun <K : Comparable<K>, V> cursorOn(name: String, vararg entries: Pair<K, V>): Cursor<K, V> {
        val list = entries.sortedBy { it.first }
        return IndexBasedCursor(
            minIndex = 0,
            maxIndex = list.lastIndex,
            getEntryAtIndex = list::get,
            name = name,
        )
    }

}