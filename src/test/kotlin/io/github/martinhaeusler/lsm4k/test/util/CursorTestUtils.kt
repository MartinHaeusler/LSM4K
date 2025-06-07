package io.github.martinhaeusler.lsm4k.test.util

import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.model.command.KeyAndTSN
import io.github.martinhaeusler.lsm4k.model.command.OpCode
import io.github.martinhaeusler.lsm4k.util.cursor.Cursor
import io.github.martinhaeusler.lsm4k.util.cursor.EmptyCursor
import io.github.martinhaeusler.lsm4k.util.cursor.IndexBasedCursor
import strikt.api.Assertion
import strikt.assertions.isEqualTo
import java.nio.charset.Charset

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

    fun <K : Comparable<K>, V> cursorOn(entries: List<Pair<K, V>>): Cursor<K, V> {
        return cursorOn("List Cursor", *entries.toTypedArray())
    }

    fun <K : Comparable<K>, V> cursorOn(name: String, vararg entries: Pair<K, V>): Cursor<K, V> {
        if (entries.isEmpty()) {
            return EmptyCursor(getCursorName = { name })
        }
        val list = entries.sortedBy { it.first }
        return IndexBasedCursor(
            minIndex = 0,
            maxIndex = list.lastIndex,
            getEntryAtIndex = list::get,
            name = name,
        )
    }

    fun KeyAndTSN.asString(charset: Charset = Charsets.UTF_8): String {
        return "${this.key.asString()}@${this.tsn}"
    }

    fun Command.asString(charset: Charset = Charsets.UTF_8): String {
        return when (this.opCode) {
            OpCode.PUT -> "${this.key.asString(charset)}@${this.tsn}->${this.value.asString(charset)}"
            OpCode.DEL -> "${this.key.asString(charset)}@${this.tsn}--"
        }
    }

    fun Assertion.Builder<Pair<KeyAndTSN, Command>>.isEqualToKeyValuePair(key: String, value: String) {
        get { this.first.asString() }.isEqualTo(key)
        get { this.second.asString() }.isEqualTo(value)
    }

}