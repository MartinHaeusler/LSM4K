package io.github.martinhaeusler.lsm4k.util.cursor

import io.github.martinhaeusler.lsm4k.util.cursor.CursorUtils.checkIsOpen

class EmptyCursor<K, V>(
    val getCursorName: () -> String,
) : CursorInternal<K, V> {

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

    override var isOpen: Boolean = true

    private val closeHandlers = mutableListOf<CloseHandler>()

    override val keyOrNull: K?
        get() {
            this.checkIsOpen()
            return null
        }

    override val valueOrNull: V?
        get() {
            this.checkIsOpen()
            return null
        }

    override val isValidPosition: Boolean
        get() = false

    override fun invalidatePositionInternal() {
        this.checkIsOpen()
    }

    override fun firstInternal(): Boolean {
        this.checkIsOpen()
        return false
    }

    override fun lastInternal(): Boolean {
        this.checkIsOpen()
        return false
    }

    override fun nextInternal(): Boolean {
        this.checkIsOpen()
        return false
    }

    override fun previousInternal(): Boolean {
        this.checkIsOpen()
        return false
    }

    override fun onClose(action: CloseHandler): Cursor<K, V> {
        this.checkIsOpen()
        this.closeHandlers += action
        return this
    }

    override fun closeInternal() {
        if (!this.isOpen) {
            return
        }
        this.isOpen = false
        CursorUtils.executeCloseHandlers(this.closeHandlers)
    }

    override fun seekExactlyOrNextInternal(key: K): Boolean {
        this.checkIsOpen()
        return false
    }

    override fun seekExactlyOrPreviousInternal(key: K): Boolean {
        this.checkIsOpen()
        return false
    }

    override fun toString(): String {
        return "EmptyCursor[${this.getCursorName()}]"
    }

}