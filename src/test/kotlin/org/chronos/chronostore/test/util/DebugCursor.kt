package org.chronos.chronostore.test.util

import io.github.oshai.kotlinlogging.KotlinLogging
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
        addCallCount(Cursor<K,V>::first)
        val success = this.innerCursor.first()
        val details = if(success){
            "[successfully moved to position before first key.]"
        }else{
            "[failed to move to position before first key. Position invalidated.]"
        }
        log.info { "'${this.name}' first() ${details}" }
        return success
    }

    override fun doLast(): Boolean {
        addCallCount(Cursor<K,V>::last)
        val success = this.innerCursor.last()
        val details = if(success){
            "[successfully moved to position after last key.]"
        }else{
            "[failed to move to position after last key. Position invalidated.]"
        }
        log.info { "'${this.name}' last() ${details}" }
        return success
    }

    override fun doMove(direction: Order): Boolean {
        val preMoveKey = this.keyOrNull
        val moveResult = this.innerCursor.move(direction)
        val postMoveKey = this.keyOrNull

        val details = if(moveResult){
            "[successfully moved: '${preMoveKey.toString().toSingleLine()}' -> '${postMoveKey.toString().toSingleLine()}']"
        }else{
            "[move not possible, cursor is at: '${postMoveKey.toString().toSingleLine()}']"
        }

        when (direction) {
            Order.ASCENDING -> {
                log.info { "'${this.name}' next() ${details}" }
                addCallCount(Cursor<K,V>::next)
            }
            Order.DESCENDING -> {
                log.info { "'${this.name}' previous() ${details}" }
                addCallCount(Cursor<K,V>::previous)
            }
        }
        return moveResult
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
        addCallCount(Cursor<K,V>::peekNext)
        val peeked = this.innerCursor.peekNext()
        val details = if(peeked == null){
            "[peek failed: no next element. Current key: '${this.keyOrNull.toString().toSingleLine()}']"
        }else{
            "[peek success. Current key: '${this.keyOrNull.toString().toSingleLine()}', peeked at key: ${peeked.first}]"
        }
        log.info { "${this.name} peekNext() ${details}" }
        return peeked
    }

    override fun peekPrevious(): Pair<K, V>? {
        addCallCount(Cursor<K,V>::peekPrevious)
        val peeked = this.innerCursor.peekPrevious()
        val details = if(peeked == null){
            "[peek failed: no next element. Current key: '${this.keyOrNull.toString().toSingleLine()}']"
        }else{
            "[peek success. Current key: '${this.keyOrNull.toString().toSingleLine()}', peeked at key: ${peeked.first}]"
        }
        log.info { "${this.name} peekPrevious() ${details}" }
        return peeked
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