package org.chronos.chronostore.util.cursor

import org.chronos.chronostore.util.cursor.CursorUtils.checkIsOpen

class MappingCursor<K, V, MK, MV>(
    private val innerCursor: CursorInternal<K, V>,
    private val mapKey: (K) -> MK,
    private val mapKeyInverse: (MK) -> K,
    private val mapValue: (V) -> MV,
) : CursorInternal<MK, MV> {

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
        get() = this.innerCursor.isOpen

    override val isValidPosition: Boolean
        get() = this.innerCursor.isValidPosition

    init {
        innerCursor.parent = this
    }

    override fun invalidatePositionInternal() {
        this.checkIsOpen()
        this.innerCursor.invalidatePositionInternal()
    }

    override fun firstInternal(): Boolean {
        this.checkIsOpen()
        return this.innerCursor.firstInternal()
    }

    override fun lastInternal(): Boolean {
        this.checkIsOpen()
        return this.innerCursor.lastInternal()
    }

    override fun nextInternal(): Boolean {
        this.checkIsOpen()
        return this.innerCursor.nextInternal()
    }

    override fun previousInternal(): Boolean {
        this.checkIsOpen()
        return this.innerCursor.previousInternal()
    }

    override val keyOrNull: MK?
        get() {
            this.checkIsOpen()
            return this.innerCursor.keyOrNull?.let(mapKey)
        }

    override val valueOrNull: MV?
        get() {
            this.checkIsOpen()
            return this.innerCursor.valueOrNull?.let(mapValue)
        }

    override fun seekExactlyOrNextInternal(key: MK): Boolean {
        this.checkIsOpen()
        return this.innerCursor.seekExactlyOrNextInternal(mapKeyInverse(key))
    }

    override fun seekExactlyOrPreviousInternal(key: MK): Boolean {
        this.checkIsOpen()
        return this.innerCursor.seekExactlyOrPreviousInternal(mapKeyInverse(key))
    }

    override fun peekNextInternal(): Pair<MK, MV>? {
        this.checkIsOpen()
        val nextEntry = this.innerCursor.peekNextInternal()
            ?: return null
        return mapKey(nextEntry.first) to mapValue(nextEntry.second)
    }

    override fun peekPrevious(): Pair<MK, MV>? {
        this.checkIsOpen()
        val previousEntry = this.innerCursor.peekPreviousInternal()
            ?: return null
        return mapKey(previousEntry.first) to mapValue(previousEntry.second)
    }

    override fun onClose(action: CloseHandler): Cursor<MK, MV> {
        this.checkIsOpen()
        this.innerCursor.onClose(action)
        return this
    }

    override fun closeInternal() {
        if (!this.isOpen) {
            return
        }
        this.innerCursor.closeInternal()
    }

    override fun toString(): String {
        return "MappingCursor[${this.innerCursor}]"
    }

}