package org.chronos.chronostore.test.util

import mu.KotlinLogging
import org.chronos.chronostore.util.Order
import org.chronos.chronostore.util.StringExtensions.toSingleLine
import org.chronos.chronostore.util.cursor.Cursor
import org.chronos.chronostore.util.cursor.WrappingCursor
import kotlin.reflect.KFunction

class DebugCursor<K : Comparable<K>, V>(
    val name: String,
    inner: Cursor<K, V>,
) : WrappingCursor<Cursor<K, V>, K, V>(inner) {

    companion object {

        private val log = KotlinLogging.logger {}

        fun <K : Comparable<K>, V> Cursor<K, V>.debug(name: String): DebugCursor<K, V> {
            return DebugCursor(name, this)
        }

    }

    val callCounts = mutableMapOf<KFunction<*>, Int>()

    override fun doFirst(): Boolean {
        log.info { "${this.name} first()" }
        addCallCount(Cursor<K,V>::first)
        return this.innerCursor.first()
    }

    override fun doLast(): Boolean {
        log.info { "${this.name} last()" }
        addCallCount(Cursor<K,V>::last)
        return this.innerCursor.last()
    }

    override fun doMove(direction: Order): Boolean {
        when (direction) {
            Order.ASCENDING -> {
                log.info { "${this.name} next()" }
                addCallCount(Cursor<K,V>::next)
            }
            Order.DESCENDING -> {
                log.info { "${this.name} previous()" }
                addCallCount(Cursor<K,V>::previous)
            }
        }
        return this.innerCursor.move(direction)
    }

    override fun doSeekExactlyOrNext(key: K): Boolean {
        log.info { "${this.name} seekExactlyOrNext(${key.toString().toSingleLine()})" }
        addCallCount(Cursor<K,V>::seekExactlyOrNext)
        return this.innerCursor.seekExactlyOrNext(key)
    }

    override fun doSeekExactlyOrPrevious(key: K): Boolean {
        log.info { "${this.name} seekExactlyOrPrevious(${key.toString().toSingleLine()})" }
        addCallCount(Cursor<K,V>::seekExactlyOrPrevious)
        return this.innerCursor.seekExactlyOrPrevious(key)
    }

    override fun peekNext(): Pair<K, V>? {
        log.info { "${this.name} peekNext()" }
        addCallCount(Cursor<K,V>::peekNext)
        return this.innerCursor.peekNext()
    }

    override fun peekPrevious(): Pair<K, V>? {
        log.info { "${this.name} peekPrevious()" }
        addCallCount(Cursor<K,V>::peekPrevious)
        return this.innerCursor.peekPrevious()
    }

    override val keyOrNullInternal: K?
        get() = this.innerCursor.keyOrNull

    override val valueOrNullInternal: V?
        get() = this.innerCursor.valueOrNull

    private fun addCallCount(call: KFunction<*>) {
        this.callCounts.compute(call) { _, oldCount ->
            (oldCount ?: 0) + 1
        }
    }

}