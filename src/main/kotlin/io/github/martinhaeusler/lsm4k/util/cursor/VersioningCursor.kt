package io.github.martinhaeusler.lsm4k.util.cursor

import io.github.martinhaeusler.lsm4k.model.command.Command
import io.github.martinhaeusler.lsm4k.model.command.KeyAndTSN
import io.github.martinhaeusler.lsm4k.model.command.OpCode
import io.github.martinhaeusler.lsm4k.util.TSN
import io.github.martinhaeusler.lsm4k.util.bytes.Bytes
import io.github.martinhaeusler.lsm4k.util.cursor.CursorUtils.checkIsOpen
import io.github.martinhaeusler.lsm4k.util.statistics.StatisticsReporter

class VersioningCursor(
    private val innerCursor: CursorInternal<KeyAndTSN, Command>,
    private val tsn: TSN,
    private val includeDeletions: Boolean,
    private val statisticsReporter: StatisticsReporter,
) : CursorInternal<Bytes, Command> {

    companion object {

        const val CURSOR_NAME = "VersioningCursor"

    }

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

    override val isOpen: Boolean
        get() {
            return this.innerCursor.isOpen
        }

    override val keyOrNull: Bytes?
        get() {
            this.checkIsOpen()
            return this.getTemporalKeyOrNull()?.key
        }

    override val valueOrNull: Command?
        get() {
            this.checkIsOpen()
            return this.innerCursor.valueOrNull
        }

    override val isValidPosition: Boolean
        get() {
            this.checkIsOpen()
            return this.innerCursor.isValidPosition
        }

    init {
        this.innerCursor.parent = this
        this.statisticsReporter.reportCursorOpened(CURSOR_NAME)
    }

    override fun invalidatePositionInternal() {
        this.checkIsOpen()
        this.innerCursor.invalidatePositionInternal()
    }

    override fun firstInternal(): Boolean {
        this.checkIsOpen()
        this.statisticsReporter.reportCursorOperationFirst(CURSOR_NAME)
        if (!this.innerCursor.firstInternal()) {
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

    override fun lastInternal(): Boolean {
        this.checkIsOpen()
        this.statisticsReporter.reportCursorOperationLast(CURSOR_NAME)
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

    override fun nextInternal(): Boolean {
        this.checkIsOpen()
        this.statisticsReporter.reportCursorOperationNext(CURSOR_NAME)
        return this.seekToNextHigherTemporalEntry()
    }

    override fun previousInternal(): Boolean {
        this.checkIsOpen()
        this.statisticsReporter.reportCursorOperationPrevious(CURSOR_NAME)
        return this.seekToNextLowerTemporalEntry()
    }


    override fun seekExactlyOrPreviousInternal(key: Bytes): Boolean {
        this.checkIsOpen()
        this.statisticsReporter.reportCursorOperationSeekExactlyOrPrevious(CURSOR_NAME)
        val temporalKey = this.convertUserKeyToUnqualifiedTemporalKey(key)

        if (!this.innerCursor.seekExactlyOrPreviousInternal(temporalKey)) {
            return false
        }
        val currentKey = this.innerCursor.key
        val nextKey = this.peekNextKey()
        if ((!includeDeletions && this.valueIsDeletionMarker()) || !this.isOutputKey(currentKey, nextKey)) {
            return this.seekToNextLowerTemporalEntry()
        }
        return true
    }

    override fun seekExactlyOrNextInternal(key: Bytes): Boolean {
        this.checkIsOpen()
        this.statisticsReporter.reportCursorOperationSeekExactlyOrNext(CURSOR_NAME)
        if (!this.innerCursor.firstInternal()) {
            return false
        }

        val temporalKey = this.convertUserKeyToUnqualifiedTemporalKey(key)

        // note: we use "seekExactlyOrPREVIOUS" on purpose on the inner cursor, because in
        // the event of an exact match, our TSN may be higher. Using "seekExactlyOrNEXT" on the
        // inner key would cause us to miss that. If the search fails on the inner cursor,
        // we know that equality is not an option (otherwise we would have found something),
        // so we retry with "seekExactlyOrNext".

        if (!this.innerCursor.seekExactlyOrPreviousInternal(temporalKey)) {
            // we found nothing with exactlyOrPrevious. We might still find something larger...
            if (!this.innerCursor.seekExactlyOrNextInternal(temporalKey)) {
                return false
            }
        } else {
            // do we have an equality match?
            val currentKey = this.innerCursor.key
            return if (currentKey.key == key) {
                // equality match with equal/lower TSN -> good!
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

    private fun getTemporalKeyOrNull(): KeyAndTSN? {
        return this.innerCursor.keyOrNull
    }

    private fun seekToNextHigherTemporalEntry(): Boolean {
        var currentKey: KeyAndTSN
        var nextHigherKey: KeyAndTSN?
        do {
            if (!this.innerCursor.nextInternal()) {
                return false
            }
            currentKey = this.getTemporalKeyOrNull()
                ?: return false
            nextHigherKey = this.peekNextKey()
        } while (!(isOutputKey(currentKey, nextHigherKey) && (this.includeDeletions || !this.valueIsDeletionMarker())))
        return true
    }

    private fun seekToNextLowerTemporalEntry(): Boolean {
        var currentKey: KeyAndTSN
        var nextHigherKey: KeyAndTSN?
        do {
            nextHigherKey = this.getTemporalKeyOrNull()
                ?: return false
            if (!this.innerCursor.previousInternal()) {
                return false
            }
            currentKey = this.getTemporalKeyOrNull()
                ?: return false
        } while (!(isOutputKey(currentKey, nextHigherKey) && (this.includeDeletions || !this.valueIsDeletionMarker())))
        return true
    }

    @Suppress("RedundantIf")
    private fun isOutputKey(currentKey: KeyAndTSN, nextHigherKey: KeyAndTSN?): Boolean {
        if (currentKey.tsn > this.tsn) {
            // the TSN of this entry is *higher* than our request TSN -> we can't see it.
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
        if (nextHigherKey.tsn > this.tsn) {
            // we can't see the next version of the same key
            // => this is the latest version of the current (user) key
            // => we need to include it in the output
            return true
        }
        return false
    }

    private fun peekNextKey(): KeyAndTSN? {
        if (this.getTemporalKeyOrNull() == null) {
            // cursor state is invalid
            return null
        }
        return this.innerCursor.peekNextInternal()?.first
    }

    private fun valueIsDeletionMarker(): Boolean {
        val currentValue = this.innerCursor.valueOrNull
            ?: return false
        return currentValue.opCode == OpCode.DEL
    }

    private fun convertUserKeyToUnqualifiedTemporalKey(userKey: Bytes): KeyAndTSN {
        return KeyAndTSN(userKey, this.tsn)
    }

    override fun closeInternal() {
        if (!this.isOpen) {
            return
        }
        this.statisticsReporter.reportCursorClosed(CURSOR_NAME)
        this.innerCursor.closeInternal()
    }

    override fun onClose(action: CloseHandler): Cursor<Bytes, Command> {
        this.checkIsOpen()
        this.innerCursor.onClose(action)
        return this
    }

    override fun toString(): String {
        return "VersioningCursor[${this.tsn} in ${this.innerCursor}]"
    }
}