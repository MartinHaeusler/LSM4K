package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.model.command.KeyAndTimestamp
import org.chronos.chronostore.util.Bytes
import org.chronos.chronostore.util.Order
import org.chronos.chronostore.util.Timestamp
import org.chronos.chronostore.util.statistics.ChronoStoreStatistics

class VersioningCursor(
    treeCursor: Cursor<KeyAndTimestamp, Command>,
    private val timestamp: Timestamp,
    private val includeDeletions: Boolean,
) : WrappingCursor<Cursor<KeyAndTimestamp, Command>, Bytes, Command>(treeCursor), Cursor<Bytes, Command> {

    init {
        ChronoStoreStatistics.VERSIONING_CURSORS.incrementAndGet()
    }

    override fun doFirst(): Boolean {
        ChronoStoreStatistics.VERSIONING_CURSOR_FIRST_SEEKS.incrementAndGet()
        if (!this.innerCursor.first()) {
            return false
        }
        // the first key in the keyspace may already be an output key. Let's check...
        val currentKey = this.innerCursor.key
        val nextKey = this.peekNextKey()
        if ((includeDeletions || !this.valueIsDeletionMarker()) && this.isOutputKey(currentKey, nextKey)) {
            return true
        }
        // the first key was not an output key; move to the first output key
        return this.seekToNextHigherTemporalEntry()
    }

    override fun doLast(): Boolean {
        ChronoStoreStatistics.VERSIONING_CURSOR_LAST_SEEKS.incrementAndGet()
        if (!this.innerCursor.last()) {
            return false
        }
        // the last key in the keyspace may already be an output key. Let's check...
        val currentKey = this.innerCursor.key
        val nextKey = null // well, there *is* no next key
        if ((includeDeletions || !this.valueIsDeletionMarker()) && isOutputKey(currentKey, nextKey)) {
            return true
        }

        // the last key was not an output key; move to the largest output key
        return this.seekToNextLowerTemporalEntry()
    }

    override fun doMove(direction: Order): Boolean {
        return when (direction) {
            Order.ASCENDING -> {
                ChronoStoreStatistics.VERSIONING_CURSOR_NEXT_SEEKS.incrementAndGet()
                this.seekToNextHigherTemporalEntry()
            }
            Order.DESCENDING -> {
                ChronoStoreStatistics.VERSIONING_CURSOR_PREVIOUS_SEEKS.incrementAndGet()
                this.seekToNextLowerTemporalEntry()
            }
        }
    }

    override fun doSeekExactlyOrPrevious(key: Bytes): Boolean {
        ChronoStoreStatistics.VERSIONING_CURSOR_EXACTLY_OR_PREVIOUS_SEEKS.incrementAndGet()
        val temporalKey = this.convertUserKeyToUnqualifiedTemporalKey(key)

        if (!this.innerCursor.seekExactlyOrPrevious(temporalKey)) {
            return false
        }
        val currentKey = this.innerCursor.key
        val nextKey = this.peekNextKey()
        if ((!includeDeletions && this.valueIsDeletionMarker()) || !this.isOutputKey(currentKey, nextKey)) {
            return this.seekToNextLowerTemporalEntry()
        }
        return true
    }

    override fun doSeekExactlyOrNext(key: Bytes): Boolean {
        ChronoStoreStatistics.VERSIONING_CURSOR_EXACTLY_OR_NEXT_SEEKS.incrementAndGet()
        if (!this.innerCursor.first()) {
            return false
        }

        val temporalKey = this.convertUserKeyToUnqualifiedTemporalKey(key)

        // note: we use "seekExactlyOrPREVIOUS" on purpose on the inner cursor, because in
        // the event of an exact match, our timestamp may be higher. Using "seekExactlyOrNEXT" on the
        // inner key would cause us to miss that. If the search fails on the inner cursor,
        // we know that equality is not an option (otherwise we would have found something),
        // so we retry with "seekExactlyOrNext".

        if (!this.innerCursor.seekExactlyOrPrevious(temporalKey)) {
            // we found nothing with exactlyOrPrevious. We might still find something larger...
            if (!this.innerCursor.seekExactlyOrNext(temporalKey)) {
                return false
            }
        } else {
            // do we have an equality match?
            val currentKey = this.innerCursor.key
            return if (currentKey.key == key) {
                // equality match with equal/lower timestamp -> good!
                true
            } else {
                // we ended up on a different key which is less than our key, scan for the next-higher entry
                this.seekToNextHigherTemporalEntry()
            }
        }
        val currentKey = this.innerCursor.key
        val nextKey = this.peekNextKey()
        if ((!includeDeletions && this.valueIsDeletionMarker()) || !this.isOutputKey(currentKey, nextKey)) {
            return this.seekToNextHigherTemporalEntry()
        }
        return true
    }

    private fun convertUserKeyToUnqualifiedTemporalKey(userKey: Bytes): KeyAndTimestamp {
        return KeyAndTimestamp(userKey, this.timestamp)
    }

    override val keyOrNullInternal: Bytes?
        get() {
            return this.getTemporalKeyOrNull()?.key
        }

    override val valueOrNullInternal: Command?
        get() {
            return this.innerCursor.valueOrNull
        }

    private fun getTemporalKeyOrNull(): KeyAndTimestamp? {
        return this.innerCursor.keyOrNull
    }

    private fun seekToNextHigherTemporalEntry(): Boolean {
        var currentKey: KeyAndTimestamp
        var nextHigherKey: KeyAndTimestamp?
        do {
            if (!this.innerCursor.next()) {
                return false
            }
            currentKey = this.getTemporalKeyOrNull()
                ?: return false
            nextHigherKey = this.peekNextKey()
        } while (!(isOutputKey(currentKey, nextHigherKey) && (this.includeDeletions || !this.valueIsDeletionMarker())))
        return true
    }

    private fun seekToNextLowerTemporalEntry(): Boolean {
        var currentKey: KeyAndTimestamp
        var nextHigherKey: KeyAndTimestamp?
        do {
            nextHigherKey = this.getTemporalKeyOrNull()
                ?: return false
            if (!this.innerCursor.previous()) {
                return false
            }
            currentKey = this.getTemporalKeyOrNull()
                ?: return false
        } while (!(isOutputKey(currentKey, nextHigherKey) && (this.includeDeletions || !this.valueIsDeletionMarker())))
        return true
    }

    @Suppress("RedundantIf")
    private fun isOutputKey(currentKey: KeyAndTimestamp, nextHigherKey: KeyAndTimestamp?): Boolean {
        if (currentKey.timestamp > this.timestamp) {
            // the timestamp of this entry is *higher* than our request timestamp -> we can't see it.
            return false
        }
        // we can see the current key, but it might not be the latest version.
        if (nextHigherKey == null || currentKey.key != nextHigherKey.key) {
            // the next entry has a different (user) key
            // => this is the latest version of the current (user) key
            // => we need to include it as output
            return true
        }
        // the next entry has the same (user) key as this entry. Check if we would be able to see the next entry
        if (nextHigherKey.timestamp > this.timestamp) {
            // we can't see the next version of the same key
            // => this is the latest version of the current (user) key
            // => we need to include it in the output
            return true
        }
        return false
    }

    private fun peekNextKey(): KeyAndTimestamp? {
        if(this.getTemporalKeyOrNull() == null){
            // cursor state is invalid
            return null
        }
        return this.innerCursor.peekNext()?.first
    }

    private fun valueIsDeletionMarker(): Boolean {
        val currentValue = this.innerCursor.valueOrNull
            ?: return false
        return currentValue.opCode == Command.OpCode.DEL
    }

    override fun toString(): String {
        return "VersioningCursor[${this.timestamp} in ${this.innerCursor}]"
    }
}