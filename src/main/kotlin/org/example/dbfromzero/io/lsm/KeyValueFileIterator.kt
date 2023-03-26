package org.example.dbfromzero.io.lsm

import org.example.dbfromzero.util.Bytes

class KeyValueFileIterator(
    private val reader: KeyValueFileReader
) : Iterator<Pair<Bytes, Bytes>>, AutoCloseable {

    private var closed: Boolean = false
    private var next: Pair<Bytes, Bytes>? = this.readNext()

    override fun hasNext(): Boolean {
        this.assertNotClosed()
        return this.next != null
    }

    override fun next(): Pair<Bytes, Bytes> {
        this.assertNotClosed()
        val current = this.next
            ?: throw NoSuchElementException("Iterator has no more elements.")
        this.next = this.readNext()
        return current
    }

    private fun readNext(): Pair<Bytes, Bytes>? {
        return this.reader.readKeyValuePair()
    }

    override fun close() {
        if (this.closed) {
            return
        }
        this.closed = true
        this.reader.close()
    }

    private fun assertNotClosed() {
        check(!this.closed) {
            "This iterator has already been closed."
        }
    }
}