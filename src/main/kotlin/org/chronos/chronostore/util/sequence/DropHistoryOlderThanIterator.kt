package org.chronos.chronostore.util.sequence

import com.google.common.collect.Iterators
import org.chronos.chronostore.model.command.Command
import org.chronos.chronostore.util.TSN

class DropHistoryOlderThanIterator(
    iterator: Iterator<Command>,
    private val minimumRequiredTSN: TSN,
): Iterator<Command> {

    private val peekingIterator = Iterators.peekingIterator(iterator)

    private var next: Command? = null

    init {
        this.moveNext()
    }

    private fun moveNext() {
        if (!this.peekingIterator.hasNext()) {
            // not found
            this.next = null
            return
        }
        var next = this.peekingIterator.next()
        while (this.peekingIterator.hasNext()) {
            val peeked = this.peekingIterator.peek()
            if (next.key == peeked.key && next.tsn < this.minimumRequiredTSN) {
                // the versions are ordered in ascending TSN order,
                // so we can be sure that the peeked version is newer.
                // Additionally, the current version is older than our
                // minimum TSN, so we can drop it.
                next = this.peekingIterator.next()
            } else {
                // we are at the latest version of the key
                break
            }
        }
        this.next = next
    }

    override fun hasNext(): Boolean {
        return this.next != null
    }

    override fun next(): Command {
        if (!this.hasNext()) {
            throw NoSuchElementException("Iterator is exhausted")
        }
        val result = this.next!!
        this.moveNext()
        return result
    }

    override fun toString(): String {
        return "DropHistoryOlderThanIterator[minimumRequiredTSN=${this.minimumRequiredTSN}]"
    }
}