package io.github.martinhaeusler.lsm4k.test.util

import io.github.martinhaeusler.lsm4k.util.StringExtensions.toSingleLine
import io.github.martinhaeusler.lsm4k.util.cursor.CloseHandler
import io.github.martinhaeusler.lsm4k.util.cursor.Cursor
import io.github.martinhaeusler.lsm4k.util.cursor.CursorInternal
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlin.reflect.KFunction

class DebugCursor<K : Comparable<K>, V>(
    val name: String,
    private val innerCursor: CursorInternal<K, V>,
) : CursorInternal<K, V> {

    companion object {

        private val log = KotlinLogging.logger {}

        fun <K : Comparable<K>, V> Cursor<K, V>.debug(name: String): DebugCursor<K, V> {
            return DebugCursor(name, this as CursorInternal<K, V>)
        }

    }

    override val isOpen: Boolean
        get() = this.innerCursor.isOpen

    override var parent: CursorInternal<*, *>? = null
        set(value) {
            if (field === value) {
                return
            }
            check(field == null) {
                "Cannot assign another parent to this cursor; a parent is already present." +
                    " Existing parent: ${field}, proposed new parent: ${value}"
            }
            field = value
        }

    override val keyOrNull: K?
        get() = this.innerCursor.keyOrNull

    override val valueOrNull: V?
        get() = this.innerCursor.valueOrNull


    override val isValidPosition: Boolean
        get() = this.innerCursor.isValidPosition

    val callCounts = mutableMapOf<KFunction<*>, Int>()

    override fun invalidatePositionInternal() {
        this.innerCursor.invalidatePositionInternal()
    }

    override fun firstInternal(): Boolean {
        addCallCount(Cursor<K, V>::first)
        val success = this.innerCursor.firstInternal()
        val details = if (success) {
            "[successfully moved to position before first key.]"
        } else {
            "[failed to move to position before first key. Position invalidated.]"
        }
        log.info { "'${this.name}' first() ${details}" }
        return success
    }

    override fun lastInternal(): Boolean {
        addCallCount(Cursor<K, V>::last)
        val success = this.innerCursor.lastInternal()
        val details = if (success) {
            "[successfully moved to position after last key.]"
        } else {
            "[failed to move to position after last key. Position invalidated.]"
        }
        log.info { "'${this.name}' last() ${details}" }
        return success
    }

    override fun nextInternal(): Boolean {
        val preMoveKey = this.keyOrNull
        val moveResult = this.innerCursor.nextInternal()
        val postMoveKey = this.keyOrNull

        val details = if (moveResult) {
            "[successfully moved: '${preMoveKey.toString().toSingleLine()}' -> '${postMoveKey.toString().toSingleLine()}']"
        } else {
            "[move not possible, cursor is at: '${postMoveKey.toString().toSingleLine()}']"
        }

        log.info { "'${this.name}' next() ${details}" }
        addCallCount(Cursor<K, V>::next)
        return moveResult
    }

    override fun previousInternal(): Boolean {
        val preMoveKey = this.keyOrNull
        val moveResult = this.innerCursor.previousInternal()
        val postMoveKey = this.keyOrNull

        val details = if (moveResult) {
            "[successfully moved: '${preMoveKey.toString().toSingleLine()}' -> '${postMoveKey.toString().toSingleLine()}']"
        } else {
            "[move not possible, cursor is at: '${postMoveKey.toString().toSingleLine()}']"
        }

        log.info { "'${this.name}' previous() ${details}" }
        addCallCount(Cursor<K, V>::previous)
        return moveResult
    }

    override fun seekExactlyOrNextInternal(key: K): Boolean {
        log.info { "${this.name} seekExactlyOrNext(${key.toString().toSingleLine()})" }
        addCallCount(Cursor<K, V>::seekExactlyOrNext)
        return this.innerCursor.seekExactlyOrNextInternal(key)
    }

    override fun seekExactlyOrPreviousInternal(key: K): Boolean {
        log.info { "${this.name} seekExactlyOrPrevious(${key.toString().toSingleLine()})" }
        addCallCount(Cursor<K, V>::seekExactlyOrPrevious)
        return this.innerCursor.seekExactlyOrPreviousInternal(key)
    }

    override fun peekNextInternal(): Pair<K, V>? {
        addCallCount(Cursor<K, V>::peekNext)
        val peeked = this.innerCursor.peekNextInternal()
        val details = if (peeked == null) {
            "[peek failed: no next element. Current key: '${this.keyOrNull.toString().toSingleLine()}']"
        } else {
            "[peek success. Current key: '${this.keyOrNull.toString().toSingleLine()}', peeked at key: ${peeked.first}]"
        }
        log.info { "${this.name} peekNext() ${details}" }
        return peeked
    }

    override fun peekPreviousInternal(): Pair<K, V>? {
        addCallCount(Cursor<K, V>::peekPrevious)
        val peeked = this.innerCursor.peekPreviousInternal()
        val details = if (peeked == null) {
            "[peek failed: no next element. Current key: '${this.keyOrNull.toString().toSingleLine()}']"
        } else {
            "[peek success. Current key: '${this.keyOrNull.toString().toSingleLine()}', peeked at key: ${peeked.first}]"
        }
        log.info { "${this.name} peekPrevious() ${details}" }
        return peeked
    }

    override fun onClose(action: CloseHandler): Cursor<K, V> {
        this.innerCursor.onClose(action)
        return this
    }

    override fun closeInternal() {
        this.innerCursor.closeInternal()
    }

    private fun addCallCount(call: KFunction<*>) {
        this.callCounts.compute(call) { _, oldCount ->
            (oldCount ?: 0) + 1
        }
    }

}